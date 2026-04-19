import React from 'react';
import {
  EdgeProps,
  getBezierPath,
  BaseEdge,
  EdgeLabelRenderer,
} from '@xyflow/react';

type CustomEdgeData = {
  absoluteFrequency?: number;
  isBottleneck?: boolean;
  label?: string;
};

const CustomEdge = (
  {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style,
    data,
    markerEnd,
  }: EdgeProps
) => {

  const typedData = data as CustomEdgeData;
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  // ✅ Stile condizionale per colli di bottiglia
  const isBottleneck = typedData?.isBottleneck === true;
  
  const edgeStyle: React.CSSProperties = {
    ...(style ?? {}),
    stroke: isBottleneck ? '#ff4d4f' : '#b1b1b7',
    strokeWidth: isBottleneck ? 3 : 1.5,
    transition: 'stroke 0.3s ease, stroke-width 0.3s ease',
  };

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={edgeStyle}
        markerEnd={markerEnd}
      />

      {/* ✅ Label tempo medio sull'arco */}
      {typedData?.label && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
              pointerEvents: 'all',
            }}
          >
            <div
              style={{
                background: isBottleneck ? '#fff2f0' : 'white',
                border: `1px solid ${isBottleneck ? '#ff4d4f' : '#e8e8ed'}`,
                borderRadius: 6,
                padding: '2px 6px',
                fontSize: '0.7rem',
                fontWeight: isBottleneck ? 600 : 400,
                color: isBottleneck ? '#cf1322' : '#595959',
                boxShadow: '0 2px 4px rgba(0,0,0,0.05)',
                userSelect: 'none',
              }}
            >
              {typedData.label}
            </div>
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
};

export default CustomEdge;