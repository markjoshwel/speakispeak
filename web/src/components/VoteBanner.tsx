import type { VoteInfo } from '../types'

interface Props {
  info: VoteInfo
}

export default function VoteBanner({ info }: Props) {
  const remaining = info.needed - info.votes
  const pct = Math.min(100, (info.votes / info.needed) * 100)

  return (
    <div className="vote-banner">
      <p className="vote-text">
        hueng... <strong>{info.voter_label}</strong> wants speaki to leave!!{' '}
        {remaining === 1
          ? 'speaki will go if one more person asks~'
          : `speaki will go if ${remaining} more people ask~`}{' '}
        ({info.votes}/{info.needed}) 🥺
      </p>
      <div className="vote-bar">
        <div className="vote-fill" style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}
