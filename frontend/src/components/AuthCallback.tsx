import React, { useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Box, Typography, CircularProgress } from '@mui/material';

const AuthCallback: React.FC = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  useEffect(() => {
    // Cattura il token dall'URL (quello lungo eyJhbGci...)
    const token = searchParams.get('token');
    
    if (token) {
      // Salva il token nel "cassetto" del browser
      localStorage.setItem('token', token); 
      // Ricarica la pagina e vai alla Dashboard
      window.location.href = '/'; 
    } else {
      console.error('Nessun token ricevuto');
      navigate('/');
    }
  }, [searchParams, navigate]);

  return (
    <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
      <CircularProgress />
      <Typography sx={{ ml: 2 }}>Completamento login in corso...</Typography>
    </Box>
  );
};

export default AuthCallback;