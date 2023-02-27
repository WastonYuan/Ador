package t_util

import (
	"os"
	"t_log"
)

type Redirect struct {
	file *(os.File)
}

func NewRedirect(filename string) *Redirect {
	f, err := os.Create(filename)
    if err != nil {
		if err := os.Truncate(filename, 0); err != nil {
			t_log.Log(t_log.ERROR, "Failed to truncate: %v", err)
		}
	}
	return &(Redirect{f})
}



func (r *Redirect) Write(content string) {
	f := r.file
	if _, err := f.Write([]byte(content)); err != nil {
		f.Close() // ignore error; Write error takes precedence
		t_log.Log(t_log.ERROR, "Failed to write: %v", err)
	}
}
