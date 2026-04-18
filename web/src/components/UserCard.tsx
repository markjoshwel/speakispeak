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
    <div id={`user-card-${user.user_id}`} className="user-row">
      <div className="user-avatar-stack">
        <div
          className="user-avatar"
          style={{
            background: userBg(user.user_id),
            borderColor: userBorder(user.user_id),
          }}
        >
          {user.avatar_url ? (
            <img src={user.avatar_url} alt={displayName} loading="lazy" />
          ) : (
            <span className="user-avatar-fallback" style={{ color: userColor(user.user_id) }}>
              {initials}
            </span>
          )}
        </div>
        <span className="user-label" style={{ color: userColor(user.user_id) }}>
          @{displayName}
        </span>
      </div>

      <div className="user-info">
        <WaveformCanvas history={user.amplitudeHistory} hue={hue} userId={user.user_id} />
      </div>
    </div>
  )
}

export default memo(UserCard)
