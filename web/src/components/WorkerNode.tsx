import { memo } from 'react'
import type { WorkerDisplayState } from '../types'
import { userColor } from '../utils'

interface Props {
  worker: WorkerDisplayState
}

function WorkerNode({ worker }: Props) {
  const active = worker.active_user_id !== null
  const workerColor = active ? userColor(worker.active_user_id!) : undefined

  return (
    <div
      id={`worker-${worker.idx}`}
      className={`worker-node${active ? ' worker-node--active' : ''}`}
      style={active ? ({ '--worker-color': workerColor } as React.CSSProperties) : undefined}
    >
      <span>{active ? 'active' : 'idle'}</span>
    </div>
  )
}

export default memo(WorkerNode)
