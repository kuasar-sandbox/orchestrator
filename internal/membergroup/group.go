package membergroup

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

type Options struct {
	Label      string
	Name       string
	Meta       Meta
	Seeds      map[string]string
	Hub        *Hub
	Log        *slog.Logger
	TLSConfig  *tls.Config
	FastTimers bool
}

type Group struct {
	label    string
	name     string
	book     *AddressBook
	delegate *delegate
	ml       *memberlist.Memberlist
	hub      *Hub
	tr       *HTTPTransport
	log      *slog.Logger
}

func New(opts Options) (*Group, error) {
	if opts.Label == "" {
		return nil, fmt.Errorf("membergroup: label is required")
	}
	if opts.Name == "" {
		return nil, fmt.Errorf("membergroup: name is required")
	}
	if opts.Hub == nil {
		opts.Hub = NewHub()
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	book := NewAddressBook(opts.Seeds)
	meta := opts.Meta
	if meta.ID == "" {
		meta.ID = opts.Name
	}
	if meta.MemberlistAdvertise != "" {
		book.Set(opts.Name, meta.MemberlistAdvertise)
	}
	d := &delegate{meta: meta, book: book, seenIDs: map[string]bool{meta.ID: true}}
	tr := NewHTTPTransport(opts.Label, opts.Name, book.Resolve, opts.TLSConfig)
	cfg := memberlist.DefaultLANConfig()
	cfg.Name = opts.Name
	cfg.Label = opts.Label
	cfg.Transport = tr
	cfg.RequireNodeNames = true
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = 1
	cfg.AdvertiseAddr = "127.0.0.1"
	cfg.AdvertisePort = 1
	cfg.Delegate = d
	cfg.Events = d
	cfg.Logger = log.New(io.Discard, "", 0)
	if opts.FastTimers {
		cfg.ProbeInterval = 200 * time.Millisecond
		cfg.ProbeTimeout = 100 * time.Millisecond
		cfg.SuspicionMult = 1
		cfg.SuspicionMaxTimeoutMult = 1
		cfg.PushPullInterval = time.Second
	}
	opts.Hub.Register(tr)
	ml, err := memberlist.Create(cfg)
	if err != nil {
		opts.Hub.Unregister(opts.Label, tr)
		_ = tr.Shutdown()
		return nil, err
	}
	return &Group{label: opts.Label, name: opts.Name, book: book, delegate: d, ml: ml, hub: opts.Hub, tr: tr, log: opts.Log}, nil
}

func (g *Group) Label() string { return g.label }
func (g *Group) Name() string  { return g.name }

func (g *Group) AddSeed(id, advertise string) {
	g.book.Set(id, advertise)
}

func (g *Group) Join(seedIDs ...string) (int, error) {
	var seeds []string
	for _, id := range seedIDs {
		if id == "" || id == g.name {
			continue
		}
		seeds = append(seeds, id+"/127.0.0.1:1")
	}
	if len(seeds) == 0 {
		return 0, nil
	}
	return g.ml.Join(seeds)
}

func (g *Group) UpdateMeta(meta Meta) error {
	if meta.ID == "" {
		meta.ID = g.name
	}
	g.delegate.setMeta(meta)
	if meta.MemberlistAdvertise != "" {
		g.book.Set(g.name, meta.MemberlistAdvertise)
	}
	return g.ml.UpdateNode(2 * time.Second)
}

func (g *Group) Members() []Meta {
	nodes := g.ml.Members()
	out := make([]Meta, 0, len(nodes))
	for _, n := range nodes {
		meta, ok := DecodeMeta(n.Meta)
		if !ok {
			meta = Meta{ID: n.Name}
		}
		if meta.ID == "" {
			meta.ID = n.Name
		}
		out = append(out, meta)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (g *Group) Alive(id string) bool {
	for _, meta := range g.Members() {
		if meta.ID == id {
			return true
		}
	}
	return id == g.name
}

func (g *Group) Seen(id string) bool {
	if id == "" {
		return false
	}
	return g.delegate.hasSeen(id)
}

func (g *Group) ReadyScalers(readyLabel string) []Meta {
	var out []Meta
	for _, meta := range g.Members() {
		if meta.Role != RoleScaler || !meta.Ready {
			continue
		}
		if readyLabel != "" && meta.ReadyLabel != readyLabel {
			continue
		}
		if meta.APIAdvertise == "" {
			continue
		}
		out = append(out, meta)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (g *Group) Shutdown() error {
	if g == nil {
		return nil
	}
	if g.hub != nil {
		g.hub.Unregister(g.label, g.tr)
	}
	if g.ml != nil {
		_ = g.ml.Leave(100 * time.Millisecond)
		return g.ml.Shutdown()
	}
	return nil
}

type delegate struct {
	mu      sync.RWMutex
	meta    Meta
	book    *AddressBook
	seenIDs map[string]bool
}

func (d *delegate) setMeta(meta Meta) {
	d.mu.Lock()
	d.meta = meta
	d.mu.Unlock()
}

func (d *delegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return encodeMeta(d.meta, limit)
}

func (d *delegate) NotifyMsg([]byte)                {}
func (d *delegate) GetBroadcasts(_, _ int) [][]byte { return nil }
func (d *delegate) LocalState(bool) []byte          { return nil }
func (d *delegate) MergeRemoteState([]byte, bool)   {}

func (d *delegate) NotifyJoin(n *memberlist.Node)   { d.learn(n) }
func (d *delegate) NotifyLeave(*memberlist.Node)    {}
func (d *delegate) NotifyUpdate(n *memberlist.Node) { d.learn(n) }

func (d *delegate) learn(n *memberlist.Node) {
	if n == nil {
		return
	}
	meta, ok := DecodeMeta(n.Meta)
	if !ok {
		return
	}
	if meta.ID == "" {
		meta.ID = n.Name
	}
	d.mu.Lock()
	if d.seenIDs == nil {
		d.seenIDs = map[string]bool{}
	}
	d.seenIDs[meta.ID] = true
	d.mu.Unlock()
	if meta.MemberlistAdvertise != "" {
		d.book.Set(meta.ID, meta.MemberlistAdvertise)
	}
}

func (d *delegate) hasSeen(id string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.seenIDs[id]
}
