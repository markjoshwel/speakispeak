import { useMemo, useEffect, useRef, memo } from 'react'
import type { UserState, TranscriptionEntry } from '../types'
import { userColor } from '../utils'

interface Props {
  user: UserState
}

function renderEntry(entry: TranscriptionEntry): React.ReactNode {
  const { text, wakeword } = entry
  if (!wakeword) return text

  const lc = text.toLowerCase()
  const kwLc = wakeword.toLowerCase()
  const idx = lc.indexOf(kwLc)
  if (idx === -1) return text

  return (
    <>
      {text.slice(0, idx)}
      <span className="tx-wakeword">{text.slice(idx, idx + wakeword.length)}</span>
      {text.slice(idx + wakeword.length)}
    </>
  )
}

function TranscriptionLine({ user }: Props) {
  const color = useMemo(() => userColor(user.user_id, 78, 0.2), [user.user_id])
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = scrollRef.current
    if (el) el.scrollLeft = el.scrollWidth
  }, [user.transcription])

  return (
    <div
      id={`tx-${user.user_id}`}
      className="tx-line"
      style={{ '--tx-color': color } as React.CSSProperties}
    >
      <div className="tx-fade-left" />
      <div className="tx-scroll" ref={scrollRef}>
        {user.transcription.length > 0 ? (
          user.transcription.map((e, i) => (
            <span key={e.id} className="tx-text">
              {i > 0 && <span className="tx-sep">　</span>}
              {renderEntry(e)}
            </span>
          ))
        ) : (
          <span className="tx-text tx-text--empty">・・・</span>
        )}
      </div>
    </div>
  )
}

export default memo(TranscriptionLine)
