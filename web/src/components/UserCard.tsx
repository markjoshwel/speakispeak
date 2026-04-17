import { useMemo, memo } from 'react'
import type { UserState } from '../types'
import { userHue, userColor, userBorder, userBg } from '../utils'
import WaveformCanvas from './WaveformCanvas'

interface Props {
  user: UserState
}

function UserCard({ user }: Props) {
  const hue = useMemo(() => userHue(user.user_id), [user.user_id])
  const displayName = user.user_label.replace(/^@/, '')
  const initials = displayName.slice(0, 2).toUpperCase()

  return (
    <div
      id={`user-card-${user.user_id}`}
      className="user-card"
      style={
        {
          '--user-color': userColor(user.user_id),
          '--user-border': userBorder(user.user_id),
          '--user-bg': userBg(user.user_id),
        } as React.CSSProperties
      }
    >
      <div className="user-avatar">
        {user.avatar_url ? (
          <img src={user.avatar_url} alt={displayName} loading="lazy" />
        ) : (
          <span className="user-avatar-fallback">{initials}</span>
        )}
      </div>

      <div className="user-info">
        <span className="user-label">@{displayName}</span>
        <WaveformCanvas history={user.amplitudeHistory} hue={hue} />
      </div>
    </div>
  )
}

export default memo(UserCard)
