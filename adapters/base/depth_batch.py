"""Atomic canonical representation for one exchange depth-update packet."""
from dataclasses import dataclass
from adapters.base.events import DepthDeltaEvent, DepthDeltaChange

@dataclass(frozen=True, slots=True)
class DepthDeltaBatchEvent:
    changes: tuple[DepthDeltaEvent, ...]
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    def __post_init__(self):
        if not isinstance(self.changes, tuple) or not self.changes:
            raise TypeError('changes must be a non-empty tuple')
        if any(not isinstance(x, DepthDeltaEvent) for x in self.changes):
            raise TypeError('changes must contain DepthDeltaEvent objects')
        for name in ('first_update_id','final_update_id','previous_final_update_id'):
            value=getattr(self,name)
            if isinstance(value,bool) or not isinstance(value,int) or value<0:
                raise ValueError(f'{name} must be a non-negative integer')
        if self.final_update_id < self.first_update_id:
            raise ValueError('final update id precedes first update id')
