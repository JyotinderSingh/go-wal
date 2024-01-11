package wal

import (
	"fmt"

	walpb "github.com/JyotinderSingh/go-wal/types"
	"google.golang.org/protobuf/proto"
)

// Marshals
func MustMarshal(entry *walpb.WAL_Entry) []byte {
	marshaledEntry, err := proto.Marshal(entry)
	if err != nil {
		panic(fmt.Sprintf("marshal should never fail (%v)", err))
	}

	return marshaledEntry
}

func MustUnmarshal(data []byte, entry *walpb.WAL_Entry) {
	if err := proto.Unmarshal(data, entry); err != nil {
		panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
	}
}
