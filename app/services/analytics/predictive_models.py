"""
Servizio Modelli Predittivi per Process Mining.

Questo modulo implementa modelli predittivi per:
- Predizione churn clienti
- Stima lifetime value
- Classificazione probabilità chiusura deal
"""

import polars as pl
import numpy as np
import pickle
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
import warnings
warnings.filterwarnings('ignore')

from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()


class PredictiveModelsService:
    """Servizio per modelli predittivi basati su event log."""
    
    def __init__(self):
        """Inizializza il servizio modelli predittivi."""
        self.models_dir = settings.processed_data_dir / "models"
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.trained_models = {}
        
        logger.info("PredictiveModelsService inizializzato")
    
    def train_model(self, 
                   df: pl.DataFrame,
                   target_variable: str,
                   model_type: str = 'random_forest',
                   hyperparameter_tuning: bool = True,
                   test_size: float = 0.2) -> Dict[str, Any]:
        """
        Allena un modello predittivo.
        
        Args:
            df: DataFrame con features
            target_variable: Variabile target
            model_type: Tipo di modello
            hyperparameter_tuning: Se eseguire hyperparameter tuning
            test_size: Percentuale dati per test
            
        Returns:
            Dizionario con risultati training
        """
        logger.info(f"Training modello {model_type} per target {target_variable}")
        
        try:
            # Prepara i dati
            X, y, feature_names = self._prepare_data(df, target_variable)
            
            if X is None or len(X) == 0:
                raise ValueError("Nessun dato disponibile per il training")
            
            # Split train/test
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=42
            )
            
            # Normalizza features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Seleziona modello
            model = self._get_model(model_type)
            
            # Hyperparameter tuning
            if hyperparameter_tuning:
                model = self._tune_hyperparameters(model, X_train_scaled, y_train, model_type)
            
            # Training
            model.fit(X_train_scaled, y_train)
            
            # Valutazione
            evaluation = self._evaluate_model(model, X_test_scaled, y_test, feature_names)
            
            # Salva modello
            model_id = self._save_model(model, model_type, target_variable, feature_names)
            
            # Risultati
            results = {
                'model_id': model_id,
                'model_type': model_type,
                'target_variable': target_variable,
                'training_samples': len(X_train),
                'test_samples': len(X_test),
                'features_used': feature_names,
                'evaluation': evaluation,
                'training_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Training completato - Accuracy: {evaluation['accuracy']:.3f}")
            return results
            
        except Exception as e:
            logger.error(f"Errore nel training del modello: {e}")
            raise
    
    def predict(self, 
               df: pl.DataFrame,
               model_id: str,
               prediction_type: str = 'classification') -> Dict[str, Any]:
        """
        Esegue predizioni con un modello addestrato.
        
        Args:
            df: DataFrame con features
            model_id: ID del modello
            prediction_type: Tipo di predizione
            
        Returns:
            Dizionario con predizioni
        """
        logger.info(f"Esecuzione predizioni con modello {model_id}")
        
        try:
            # Carica modello
            model_data = self._load_model(model_id)
            model = model_data['model']
            feature_names = model_data['features']
            
            # Prepara features
            X = self._extract_features_for_prediction(df, feature_names)
            
            if X is None or len(X) == 0:
                raise ValueError("Nessun dato disponibile per le predizioni")
            
            # Normalizza
            X_scaled = self.scaler.transform(X)
            
            # Predizioni
            predictions = model.predict(X_scaled)
            
            # Probabilità (se classificatore)
            probabilities = None
            if hasattr(model, 'predict_proba'):
                probabilities = model.predict_proba(X_scaled)
            
            # Risultati
            results = {
                'model_id': model_id,
                'prediction_type': prediction_type,
                'predictions': predictions.tolist(),
                'probabilities': probabilities.tolist() if probabilities is not None else None,
                'confidence_scores': self._calculate_confidence(predictions, probabilities),
                'total_predictions': len(predictions),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Predizioni completate: {len(predictions)} campioni")
            return results
            
        except Exception as e:
            logger.error(f"Errore nelle predizioni: {e}")
            raise
    
    def get_model_performance(self, model_id: str) -> Dict[str, Any]:
        """
        Ottiene le performance di un modello.
        
        Args:
            model_id: ID del modello
            
        Returns:
            Dizionario con performance
        """
        try:
            model_data = self._load_model(model_id)
            
            return {
                'model_id': model_id,
                'model_type': model_data['model_type'],
                'target_variable': model_data['target_variable'],
                'performance_metrics': model_data.get('evaluation', {}),
                'feature_importance': model_data.get('feature_importance', []),
                'training_timestamp': model_data.get('training_timestamp'),
                'last_used': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero performance modello: {e}")
            return {'error': str(e)}
    
    def list_available_models(self) -> List[Dict[str, Any]]:
        """
        Lista tutti i modelli disponibili.
        
        Returns:
            Lista di modelli
        """
        try:
            models = []
            
            for model_file in self.models_dir.glob("*.pkl"):
                try:
                    with open(model_file, 'rb') as f:
                        model_data = pickle.load(f)
                    
                    models.append({
                        'model_id': model_file.stem,
                        'model_type': model_data.get('model_type', 'unknown'),
                        'target_variable': model_data.get('target_variable', 'unknown'),
                        'features_count': len(model_data.get('features', [])),
                        'training_timestamp': model_data.get('training_timestamp'),
                        'file_size': model_file.stat().st_size
                    })
                except Exception as e:
                    logger.warning(f"Errore nel caricamento modello {model_file}: {e}")
                    continue
            
            return models
            
        except Exception as e:
            logger.error(f"Errore nella lista modelli: {e}")
            return []
    
    def _prepare_data(self, df: pl.DataFrame, target_variable: str) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], List[str]]:
        """Prepara i dati per il training."""
        try:
            if target_variable not in df.columns:
                logger.error(f"Variabile target {target_variable} non trovata")
                return None, None, []
            
            # Rimuovi righe con target nullo
            df_clean = df.filter(pl.col(target_variable).is_not_null())
            
            if df_clean.is_empty():
                logger.error("Nessun dato valido dopo pulizia")
                return None, None, []
            
            # Seleziona solo colonne numeriche per features
            numeric_columns = []
            for col in df_clean.columns:
                if col != target_variable:
                    dtype = df_clean[col].dtype
                    if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                        numeric_columns.append(col)
            
            if not numeric_columns:
                logger.error("Nessuna feature numerica trovata")
                return None, None, []
            
            # Estrai features e target
            X = df_clean.select(numeric_columns).to_numpy()
            y = df_clean[target_variable].to_numpy()
            
            # Gestisci valori mancanti
            X = np.nan_to_num(X, nan=0.0)
            
            return X, y, numeric_columns
            
        except Exception as e:
            logger.error(f"Errore nella preparazione dati: {e}")
            return None, None, []
    
    def _get_model(self, model_type: str):
        """Ottiene un'istanza del modello specificato."""
        if model_type == 'random_forest':
            return RandomForestClassifier(n_estimators=100, random_state=42)
        elif model_type == 'gradient_boosting':
            return GradientBoostingClassifier(n_estimators=100, random_state=42)
        else:
            logger.warning(f"Tipo modello {model_type} non supportato, uso Random Forest")
            return RandomForestClassifier(n_estimators=100, random_state=42)
    
    def _tune_hyperparameters(self, model, X_train, y_train, model_type: str):
        """Esegue hyperparameter tuning."""
        try:
            if model_type == 'random_forest':
                param_grid = {
                    'n_estimators': [50, 100, 200],
                    'max_depth': [5, 10, 20, None],
                    'min_samples_split': [2, 5, 10]
                }
            elif model_type == 'gradient_boosting':
                param_grid = {
                    'n_estimators': [50, 100, 200],
                    'learning_rate': [0.01, 0.1, 0.2],
                    'max_depth': [3, 5, 10]
                }
            else:
                return model
            
            grid_search = GridSearchCV(
                model, param_grid, cv=5, scoring='accuracy', n_jobs=-1
            )
            grid_search.fit(X_train, y_train)
            
            logger.info(f"Migliori parametri: {grid_search.best_params_}")
            return grid_search.best_estimator_
            
        except Exception as e:
            logger.warning(f"Errore nell'hyperparameter tuning: {e}")
            return model
    
    def _evaluate_model(self, model, X_test, y_test, feature_names: List[str]) -> Dict[str, Any]:
        """Valuta le performance del modello."""
        try:
            y_pred = model.predict(X_test)
            
            # Calcola metriche
            accuracy = accuracy_score(y_test, y_pred)
            
            # Gestisci caso multiclass
            average_method = 'weighted' if len(np.unique(y_test)) > 2 else 'binary'
            
            precision = precision_score(y_test, y_pred, average=average_method, zero_division=0)
            recall = recall_score(y_test, y_pred, average=average_method, zero_division=0)
            f1 = f1_score(y_test, y_pred, average=average_method, zero_division=0)
            
            # ROC AUC (solo per classificazione binaria)
            roc_auc = None
            if len(np.unique(y_test)) == 2:
                try:
                    y_pred_proba = model.predict_proba(X_test)[:, 1]
                    roc_auc = roc_auc_score(y_test, y_pred_proba)
                except:
                    pass
            
            # Feature importance
            feature_importance = []
            if hasattr(model, 'feature_importances_'):
                importances = model.feature_importances_
                for name, importance in zip(feature_names, importances):
                    feature_importance.append({
                        'feature': name,
                        'importance': float(importance)
                    })
                feature_importance.sort(key=lambda x: x['importance'], reverse=True)
            
            return {
                'accuracy': float(accuracy),
                'precision': float(precision),
                'recall': float(recall),
                'f1_score': float(f1),
                'roc_auc': float(roc_auc) if roc_auc is not None else None,
                'feature_importance': feature_importance[:10]  # Top 10
            }
            
        except Exception as e:
            logger.error(f"Errore nella valutazione modello: {e}")
            return {'error': str(e)}
    
    def _save_model(self, model, model_type: str, target_variable: str, feature_names: List[str]) -> str:
        """Salva il modello addestrato."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            model_id = f"{model_type}_{target_variable}_{timestamp}"
            
            model_data = {
                'model': model,
                'model_type': model_type,
                'target_variable': target_variable,
                'features': feature_names,
                'scaler': self.scaler,
                'training_timestamp': datetime.now().isoformat()
            }
            
            model_path = self.models_dir / f"{model_id}.pkl"
            with open(model_path, 'wb') as f:
                pickle.dump(model_data, f)
            
            logger.info(f"Modello salvato: {model_path}")
            return model_id
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio modello: {e}")
            raise
    
    def _load_model(self, model_id: str) -> Dict[str, Any]:
        """Carica un modello salvato."""
        model_path = self.models_dir / f"{model_id}.pkl"
        
        if not model_path.exists():
            raise FileNotFoundError(f"Modello {model_id} non trovato")
        
        with open(model_path, 'rb') as f:
            return pickle.load(f)
    
    def _extract_features_for_prediction(self, df: pl.DataFrame, feature_names: List[str]) -> Optional[np.ndarray]:
        """Estrae le features per le predizioni."""
        try:
            available_features = [f for f in feature_names if f in df.columns]
            
            if not available_features:
                logger.error("Nessuna feature disponibile per le predizioni")
                return None
            
            X = df.select(available_features).to_numpy()
            X = np.nan_to_num(X, nan=0.0)
            
            return X
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione features: {e}")
            return None
    
    def _calculate_confidence(self, predictions: np.ndarray, probabilities: Optional[np.ndarray]) -> List[float]:
        """Calcola score di confidenza per le predizioni."""
        if probabilities is None:
            return [1.0] * len(predictions)
        
        # Confidence = max probability per prediction
        confidence = np.max(probabilities, axis=1).tolist()
        return confidence


# Istanza globale
predictive_models_service = PredictiveModelsService()