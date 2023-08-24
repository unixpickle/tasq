package main

import (
	"context"
	"encoding/json"
	"io"
)

type JSONWriter interface {
	WriteJSON(w io.Writer) error
}

func WriteJSONObject(w io.Writer, obj map[string]interface{}) error {
	if _, err := w.Write([]byte("{")); err != nil {
		return err
	}
	first := true
	for k, v := range obj {
		if first {
			first = false
		} else {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}

		// There are probably some extra restrictions on JSON keys, but
		// we ignore these for now.
		encKey, err := json.Marshal(k)
		if err != nil {
			return err
		}
		if _, err := w.Write(append(encKey, ':')); err != nil {
			return err
		}

		if writer, ok := v.(JSONWriter); ok {
			if err := writer.WriteJSON(w); err != nil {
				return err
			}
		} else {
			encoded, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if _, err := w.Write(encoded); err != nil {
				return err
			}
		}
	}
	if _, err := w.Write([]byte("}")); err != nil {
		return err
	}
	return nil
}

type EncodedTaskList []EncodedTask

func (e EncodedTaskList) WriteJSON(w io.Writer) error {
	encodedStream := make(chan []byte, 32)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer close(encodedStream)
		for _, t := range e {
			data, err := json.Marshal(t)
			if err != nil {
				panic(err)
			}
			select {
			case encodedStream <- data:
			case <-ctx.Done():
				return
			}
		}
	}()

	first := true
	if _, err := w.Write([]byte("[")); err != nil {
		return err
	}
	for encoded := range encodedStream {
		if first {
			first = false
		} else {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		if _, err := w.Write(encoded); err != nil {
			return err
		}
	}
	if _, err := w.Write([]byte("]")); err != nil {
		return err
	}
	return nil
}
