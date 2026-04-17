import { useMemo, memo } from 'react'
import type { UserState } from '../types'
import { userHue, userColor } from '../utils'
import WaveformCanvas from './WaveformCanvas'

interface Props {
  user: UserState
}

function UserCard({ user }: Props) {
  const hue = useMemo(() => userHue(user.user_id), [user.user_id])
  const displayName = user.user_label.replace(/^@/, '')

  return (
    <div
      id={`user-card-${user.user_id}`}
      className="user-row"
    >
      <span className="user-label" style={{ color: userColor(user.user_id) }}>
        @{displayName}
      </span>
      <WaveformCanvas history={user.amplitudeHistory} hue={hue} userId={user.user_id} />
    </div>
  )
}

export default memo(UserCard)
