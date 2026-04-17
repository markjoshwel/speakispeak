import { useMemo, useEffect, useRef, memo } from 'react'
import type { UserState } from '../types'
import { userColor } from '../utils'

interface Props {
  user: UserState
}

function TranscriptionLine({ user }: Props) {
  const color = useMemo(() => userColor(user.user_id, 78, 0.2), [user.user_id])
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = scrollRef.current
    if (el) el.scrollLeft = el.scrollWidth
  }, [user.transcription])

  const fullText = user.transcription.map((e) => e.text).join('　')

  return (
    <div
      id={`tx-${user.user_id}`}
      className="tx-line"
      style={{ '--tx-color': color } as React.CSSProperties}
    >
      <div className="tx-fade-left" />
      <div className="tx-scroll" ref={scrollRef}>
        <span className="tx-text">{fullText || '・・・'}</span>
      </div>
    </div>
  )
}

export default memo(TranscriptionLine)
