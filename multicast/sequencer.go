package multicast

import (
	"context"
	"time"
)

type pendingWrite struct {
	command Command
	ctx     context.Context
	done    chan writeResult
}
type writeResult struct {
	result Result
	err    error
}

// One owner assigns batch sequence numbers. There is no automatic promotion.
func (p *Consistent) runSequencer() {
	defer close(p.stopped)
	for {
		var first pendingWrite
		select {
		case <-p.ctx.Done():
			return
		case first = <-p.queue:
		}
		writes := []pendingWrite{first}
		timer := time.NewTimer(p.flush)
	collect:
		for len(writes) < p.batchSize {
			select {
			case w := <-p.queue:
				writes = append(writes, w)
			case <-timer.C:
				break collect
			case <-p.ctx.Done():
				timer.Stop()
				return
			}
		}
		timer.Stop()
		live := writes[:0]
		for _, w := range writes {
			if err := w.ctx.Err(); err != nil {
				w.done <- writeResult{err: err}
			} else {
				live = append(live, w)
			}
		}
		if len(live) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
		select {
		case p.order <- struct{}{}:
		case <-ctx.Done():
			for _, w := range live {
				w.done <- writeResult{err: ctx.Err()}
			}
			cancel()
			continue
		}
		err := p.synchronize(ctx)
		if err == nil {
			seq, hash, e := p.State.Position()
			err = e
			if err == nil {
				commands := make([]Command, 0, len(live))
				for _, w := range live {
					_, known, e := p.State.Receipt(w.command)
					if e != nil {
						err = e
						break
					}
					if !known {
						commands = append(commands, w.command)
					}
				}
				if err == nil && len(commands) > 0 {
					record := Record{Sequence: seq + 1, Previous: hash, Batch: commands}
					record.Hash = record.Digest()
					err = p.State.Deliver(record)
					if err == nil {
						err = p.synchronize(ctx)
					}
				}
			}
		}
		for _, w := range live {
			result := Result{}
			resultErr := err
			if err == nil {
				result, _, resultErr = p.State.Receipt(w.command)
			}
			w.done <- writeResult{result: result, err: resultErr}
		}
		<-p.order
		cancel()
	}
}
