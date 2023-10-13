package utils

import (
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

func NewProgressInstance() *mpb.Progress {
	return mpb.New(mpb.WithWidth(64))
}

func NewProgressBar(instance *mpb.Progress, label string, size int) *mpb.Bar {
	bar := instance.AddBar(int64(size),
		mpb.PrependDecorators(
			decor.Name(label, decor.WC{W: len(label) + 1, C: decor.DidentRight}),
			decor.CountersNoUnit("%d/%d"),
			decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}),
		),
		mpb.AppendDecorators(
			decor.OnComplete(
				decor.Percentage(decor.WC{W: 4}), "done",
			),
		),
	)

	return bar
}