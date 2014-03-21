// this is adapted from https://github.com/alk/maxi
package main

import (
	"encoding/binary"
	"flag"
	"log"
	"strconv"
	"time"
)

import (
	"github.com/aartamonau/bgfetches/core"
	"github.com/aartamonau/bgfetches/memcached"
	"github.com/dustin/randbo"
)

var (
	rand = randbo.New()
	ZeroFlagsExpiration [8]byte
)

var (
	sinkURL = flag.String("sinkURL",
		"http://localhost:9000/", "Couchbase URL i.e. http://<host>:8091/")
	numItems       = flag.Int("numItems", 100000, "Number of items to create")
	numConnections = flag.Int("numConnections", 1, "Number of connections to use")
	sets           = flag.Bool("sets", false, "Create items")
	evicts         = flag.Bool("evicts", false, "Evict items")
	gets           = flag.Bool("gets", false, "Fetch previously created items")
	valueSize      = flag.Int("value", 1024, "Value size")
)

type loaderReq struct {
	respChan core.SinkChan
	req      *memcached.MCRequest
}

func runRepliesReader(sink core.MCDSink, sentReqs chan loaderReq,
	sinkChanBuf chan core.SinkChan) {

	for sreq := range sentReqs {
		mcresp := <-sreq.respChan
		if mcresp == nil {
			log.Printf("Got bad response")
			break
		}
		mcreq := sreq.req
		status := mcresp.Status
		if status == memcached.TMPFAIL {
			// log.Printf("tmpfail for %v", *mcreq)
			// TODO: we're kinda silly here. Need delay
			// only on that server's queue
			time.Sleep(10 * time.Millisecond)
			sink.SendRequest(mcreq, sreq.respChan)
			sentReqs <- sreq
		} else {
			if status == memcached.SUCCESS {
				// log.Printf("Got ok reply for %v", *mcreq)
			} else if status == memcached.KEY_EEXISTS {
				// ignore
			} else {
				log.Printf("Got error for %v: %v", *mcreq, *mcresp)
			}
			// log.Printf("rep-reader: Sending back: %p", sreq.respChan)
			sinkChanBuf <- sreq.respChan
		}
	}
}

func encodeFlagsExpiration(flags uint32, expiration uint32) []byte {
	if flags == 0 && expiration == 0 {
		return ZeroFlagsExpiration[:]
	}
	rv := make([]byte, 8)
	binary.BigEndian.PutUint32(rv, flags)
	binary.BigEndian.PutUint32(rv[4:], expiration)
	return rv
}

func buildSetRequest(code memcached.CommandCode, key []byte, body []byte, flags uint32, expiration uint32) (rv memcached.MCRequest) {
	rv.Opcode = code
	rv.Key = key
	rv.Body = body
	if code != memcached.APPEND {
		rv.Extras = encodeFlagsExpiration(flags, expiration)
	}
	return
}

func buildGetRequest(key []byte) (rv memcached.MCRequest) {
	rv.Opcode = memcached.GET
	rv.Key = key
	return
}

func runKeys(fn func (_ []byte)) {
	for i := 0; i < *numItems; i++ {
		key := []byte(strconv.Itoa(i))
		fn(key)
	}
}

func runKVs(fn func (_, _ []byte)) {
	for i := 0; i < *numItems; i++ {
		value := make([]byte, *valueSize)
		key := []byte(strconv.Itoa(i))
		if _, err := rand.Read(value); err != nil {
			panic(err)
		}

		fn(key, value)
	}
}

func runSets(sink core.MCDSink) {
	sinkChanBuf := make(chan core.SinkChan, core.QueueDepth)
	for i := 0; i < cap(sinkChanBuf); i++ {
		sinkChanBuf <- make(core.SinkChan, 1)
	}

	sentReqs := make(chan loaderReq, core.QueueDepth)
	go runRepliesReader(sink, sentReqs, sinkChanBuf)

	runKVs(func (key, value []byte) {
		rch := <-sinkChanBuf
		mcreq := buildSetRequest(memcached.SET, key, value, 0, 0)
		sink.SendRequest(&mcreq, rch)
		sentReqs <- loaderReq{
			req:      &mcreq,
			respChan: rch,
		}
	})
	close(sentReqs)

	for i := 0; i < cap(sinkChanBuf); i++ {
		_ = <-sinkChanBuf
	}
}

type waitCBType struct {
	num int
	done chan struct {}

	started, reported time.Time
}

func makeWaitCBType() (done chan struct {}, cb *waitCBType) {
	done = make(chan struct {})
	ts := time.Now()
	cb = &waitCBType{
		num: *numItems,
		done: done,
		started: ts,
		reported: ts,
	}

	return
}

func (cb *waitCBType) OnResponse(_ *memcached.MCRequest, _ *memcached.MCResponse) {
	cb.num--
	if cb.num == 0 {
		close(cb.done)
	}

	if time.Since(cb.reported).Seconds() >= 5 {
		cb.reported = time.Now()
		log.Printf("Processed %d items in %f seconds",
			*numItems - cb.num, time.Since(cb.started).Seconds())
	}
}

func runEvicts(sink core.MCDSink) {
	done, cb := makeWaitCBType()

	runKeys(func (key []byte) {
		mcreq := memcached.MCRequest{
			Opcode: memcached.EVICT_KEY,
			Key:    key,
		}
		sink.SendRequest(&mcreq, cb)
	})

	_ = <-done
}

func runGets(sink core.MCDSink) {
	done, cb := makeWaitCBType()

	runKeys(func (key []byte) {
		mcreq := buildGetRequest(key)
		sink.SendRequest(&mcreq, cb)
	})

	_ = <-done
}

func main() {
	flag.Parse()

	sink, err := core.NewCouchbaseSink(*sinkURL, "default", *numConnections)
	if err != nil {
		panic(err)
	}

	if *sets {
		log.Printf("Starting sending sets")

		setsStart := time.Now()
		runSets(sink)
		log.Printf("Processed all sets (%f per second)",
			float64(*numItems) / time.Since(setsStart).Seconds())
	} else if *evicts {
		log.Printf("Starting sending evicts")
		evictsStart := time.Now()
		runEvicts(sink)
		log.Printf("Processed all evicts (%f per second)",
			float64(*numItems) / time.Since(evictsStart).Seconds())
	} else if *gets {
		log.Printf("Starting sending gets")

		getsStart := time.Now()
		runGets(sink)
		log.Printf("Processed all sets (%f per second)",
			float64(*numItems) / time.Since(getsStart).Seconds())
	} else {
		log.Printf("Have nothing to do. Exiting")
		return
	}
}
