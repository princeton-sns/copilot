package viewchangeproto

import (
	"fastrpc"
	"io"
	"sync"
)

func (t *View) New() fastrpc.Serializable {
	return new(View)
}

func (t *ViewChange) New() fastrpc.Serializable {
	return new(ViewChange)
}

func (t *ViewChangeOK) New() fastrpc.Serializable {
	return new(ViewChangeOK)
}

func (t *ViewChangeReject) New() fastrpc.Serializable {
	return new(ViewChangeReject)
}

func (t *ViewChangeReply) New() fastrpc.Serializable {
	return new(ViewChangeReply)
}

func (t *AcceptView) New() fastrpc.Serializable {
	return new(AcceptView)
}

func (t *AcceptViewReply) New() fastrpc.Serializable {
	return new(AcceptViewReply)
}

func (t *InitView) New() fastrpc.Serializable {
	return new(InitView)
}

func (t *AcceptViewReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 9, true
}

type AcceptViewReplyCache struct {
	mu	sync.Mutex
	cache	[]*AcceptViewReply
}

func NewAcceptViewReplyCache() *AcceptViewReplyCache {
	c := &AcceptViewReplyCache{}
	c.cache = make([]*AcceptViewReply, 0)
	return c
}

func (p *AcceptViewReplyCache) Get() *AcceptViewReply {
	var t *AcceptViewReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AcceptViewReply{}
	}
	return t
}
func (p *AcceptViewReplyCache) Put(t *AcceptViewReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *AcceptViewReply) Marshal(wire io.Writer) {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.PilotId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.ViewId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	bs[8] = byte(t.OK)
	wire.Write(bs)
}

func (t *AcceptViewReply) Unmarshal(wire io.Reader) error {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.PilotId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ViewId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.OK = uint8(bs[8])
	return nil
}

func (t *View) BinarySize() (nbytes int, sizeKnown bool) {
	return 12, true
}

type ViewCache struct {
	mu	sync.Mutex
	cache	[]*View
}

func NewViewCache() *ViewCache {
	c := &ViewCache{}
	c.cache = make([]*View, 0)
	return c
}

func (p *ViewCache) Get() *View {
	var t *View
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &View{}
	}
	return t
}
func (p *ViewCache) Put(t *View) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *View) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.ViewId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.PilotId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.ReplicaId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *View) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.ViewId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.PilotId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.ReplicaId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	return nil
}

func (t *ViewChangeReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 45, true
}

type ViewChangeReplyCache struct {
	mu	sync.Mutex
	cache	[]*ViewChangeReply
}

func NewViewChangeReplyCache() *ViewChangeReplyCache {
	c := &ViewChangeReplyCache{}
	c.cache = make([]*ViewChangeReply, 0)
	return c
}

func (p *ViewChangeReplyCache) Get() *ViewChangeReply {
	var t *ViewChangeReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ViewChangeReply{}
	}
	return t
}
func (p *ViewChangeReplyCache) Put(t *ViewChangeReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ViewChangeReply) Marshal(wire io.Writer) {
	var b [45]byte
	var bs []byte
	bs = b[:45]
	tmp32 := t.PilotId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.ViewId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.MaxSeenViewId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	bs[12] = byte(t.OK)
	tmp32 = t.LatestCommittedInstance
	bs[13] = byte(tmp32)
	bs[14] = byte(tmp32 >> 8)
	bs[15] = byte(tmp32 >> 16)
	bs[16] = byte(tmp32 >> 24)
	tmp32 = t.LatestInstance
	bs[17] = byte(tmp32)
	bs[18] = byte(tmp32 >> 8)
	bs[19] = byte(tmp32 >> 16)
	bs[20] = byte(tmp32 >> 24)
	tmp32 = t.AcceptedView.ViewId
	bs[21] = byte(tmp32)
	bs[22] = byte(tmp32 >> 8)
	bs[23] = byte(tmp32 >> 16)
	bs[24] = byte(tmp32 >> 24)
	tmp32 = t.AcceptedView.PilotId
	bs[25] = byte(tmp32)
	bs[26] = byte(tmp32 >> 8)
	bs[27] = byte(tmp32 >> 16)
	bs[28] = byte(tmp32 >> 24)
	tmp32 = t.AcceptedView.ReplicaId
	bs[29] = byte(tmp32)
	bs[30] = byte(tmp32 >> 8)
	bs[31] = byte(tmp32 >> 16)
	bs[32] = byte(tmp32 >> 24)
	tmp32 = t.OldView.ViewId
	bs[33] = byte(tmp32)
	bs[34] = byte(tmp32 >> 8)
	bs[35] = byte(tmp32 >> 16)
	bs[36] = byte(tmp32 >> 24)
	tmp32 = t.OldView.PilotId
	bs[37] = byte(tmp32)
	bs[38] = byte(tmp32 >> 8)
	bs[39] = byte(tmp32 >> 16)
	bs[40] = byte(tmp32 >> 24)
	tmp32 = t.OldView.ReplicaId
	bs[41] = byte(tmp32)
	bs[42] = byte(tmp32 >> 8)
	bs[43] = byte(tmp32 >> 16)
	bs[44] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ViewChangeReply) Unmarshal(wire io.Reader) error {
	var b [45]byte
	var bs []byte
	bs = b[:45]
	if _, err := io.ReadAtLeast(wire, bs, 45); err != nil {
		return err
	}
	t.PilotId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ViewId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.MaxSeenViewId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.OK = uint8(bs[12])
	t.LatestCommittedInstance = int32((uint32(bs[13]) | (uint32(bs[14]) << 8) | (uint32(bs[15]) << 16) | (uint32(bs[16]) << 24)))
	t.LatestInstance = int32((uint32(bs[17]) | (uint32(bs[18]) << 8) | (uint32(bs[19]) << 16) | (uint32(bs[20]) << 24)))
	t.AcceptedView.ViewId = int32((uint32(bs[21]) | (uint32(bs[22]) << 8) | (uint32(bs[23]) << 16) | (uint32(bs[24]) << 24)))
	t.AcceptedView.PilotId = int32((uint32(bs[25]) | (uint32(bs[26]) << 8) | (uint32(bs[27]) << 16) | (uint32(bs[28]) << 24)))
	t.AcceptedView.ReplicaId = int32((uint32(bs[29]) | (uint32(bs[30]) << 8) | (uint32(bs[31]) << 16) | (uint32(bs[32]) << 24)))
	t.OldView.ViewId = int32((uint32(bs[33]) | (uint32(bs[34]) << 8) | (uint32(bs[35]) << 16) | (uint32(bs[36]) << 24)))
	t.OldView.PilotId = int32((uint32(bs[37]) | (uint32(bs[38]) << 8) | (uint32(bs[39]) << 16) | (uint32(bs[40]) << 24)))
	t.OldView.ReplicaId = int32((uint32(bs[41]) | (uint32(bs[42]) << 8) | (uint32(bs[43]) << 16) | (uint32(bs[44]) << 24)))
	return nil
}

func (t *InitView) BinarySize() (nbytes int, sizeKnown bool) {
	return 20, true
}

type InitViewCache struct {
	mu	sync.Mutex
	cache	[]*InitView
}

func NewInitViewCache() *InitViewCache {
	c := &InitViewCache{}
	c.cache = make([]*InitView, 0)
	return c
}

func (p *InitViewCache) Get() *InitView {
	var t *InitView
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &InitView{}
	}
	return t
}
func (p *InitViewCache) Put(t *InitView) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *InitView) Marshal(wire io.Writer) {
	var b [20]byte
	var bs []byte
	bs = b[:20]
	tmp32 := t.PilotId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.NewView.ViewId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.NewView.PilotId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.NewView.ReplicaId
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = t.LatestInstance
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *InitView) Unmarshal(wire io.Reader) error {
	var b [20]byte
	var bs []byte
	bs = b[:20]
	if _, err := io.ReadAtLeast(wire, bs, 20); err != nil {
		return err
	}
	t.PilotId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.NewView.ViewId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.NewView.PilotId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.NewView.ReplicaId = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.LatestInstance = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	return nil
}

func (t *ViewChange) BinarySize() (nbytes int, sizeKnown bool) {
	return 24, true
}

type ViewChangeCache struct {
	mu	sync.Mutex
	cache	[]*ViewChange
}

func NewViewChangeCache() *ViewChangeCache {
	c := &ViewChangeCache{}
	c.cache = make([]*ViewChange, 0)
	return c
}

func (p *ViewChangeCache) Get() *ViewChange {
	var t *ViewChange
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ViewChange{}
	}
	return t
}
func (p *ViewChangeCache) Put(t *ViewChange) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ViewChange) Marshal(wire io.Writer) {
	var b [24]byte
	var bs []byte
	bs = b[:24]
	tmp32 := t.ViewManagerId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.PilotId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.OldView.ViewId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.OldView.PilotId
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = t.OldView.ReplicaId
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	tmp32 = t.NewViewId
	bs[20] = byte(tmp32)
	bs[21] = byte(tmp32 >> 8)
	bs[22] = byte(tmp32 >> 16)
	bs[23] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ViewChange) Unmarshal(wire io.Reader) error {
	var b [24]byte
	var bs []byte
	bs = b[:24]
	if _, err := io.ReadAtLeast(wire, bs, 24); err != nil {
		return err
	}
	t.ViewManagerId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.PilotId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.OldView.ViewId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.OldView.PilotId = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.OldView.ReplicaId = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	t.NewViewId = int32((uint32(bs[20]) | (uint32(bs[21]) << 8) | (uint32(bs[22]) << 16) | (uint32(bs[23]) << 24)))
	return nil
}

func (t *ViewChangeOK) BinarySize() (nbytes int, sizeKnown bool) {
	return 28, true
}

type ViewChangeOKCache struct {
	mu	sync.Mutex
	cache	[]*ViewChangeOK
}

func NewViewChangeOKCache() *ViewChangeOKCache {
	c := &ViewChangeOKCache{}
	c.cache = make([]*ViewChangeOK, 0)
	return c
}

func (p *ViewChangeOKCache) Get() *ViewChangeOK {
	var t *ViewChangeOK
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ViewChangeOK{}
	}
	return t
}
func (p *ViewChangeOKCache) Put(t *ViewChangeOK) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ViewChangeOK) Marshal(wire io.Writer) {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	tmp32 := t.PilotId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.ViewId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.LatestCommittedInstance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.LatestInstance
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = t.AcceptedView.ViewId
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	tmp32 = t.AcceptedView.PilotId
	bs[20] = byte(tmp32)
	bs[21] = byte(tmp32 >> 8)
	bs[22] = byte(tmp32 >> 16)
	bs[23] = byte(tmp32 >> 24)
	tmp32 = t.AcceptedView.ReplicaId
	bs[24] = byte(tmp32)
	bs[25] = byte(tmp32 >> 8)
	bs[26] = byte(tmp32 >> 16)
	bs[27] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ViewChangeOK) Unmarshal(wire io.Reader) error {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	if _, err := io.ReadAtLeast(wire, bs, 28); err != nil {
		return err
	}
	t.PilotId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ViewId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.LatestCommittedInstance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.LatestInstance = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.AcceptedView.ViewId = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	t.AcceptedView.PilotId = int32((uint32(bs[20]) | (uint32(bs[21]) << 8) | (uint32(bs[22]) << 16) | (uint32(bs[23]) << 24)))
	t.AcceptedView.ReplicaId = int32((uint32(bs[24]) | (uint32(bs[25]) << 8) | (uint32(bs[26]) << 16) | (uint32(bs[27]) << 24)))
	return nil
}

func (t *ViewChangeReject) BinarySize() (nbytes int, sizeKnown bool) {
	return 24, true
}

type ViewChangeRejectCache struct {
	mu	sync.Mutex
	cache	[]*ViewChangeReject
}

func NewViewChangeRejectCache() *ViewChangeRejectCache {
	c := &ViewChangeRejectCache{}
	c.cache = make([]*ViewChangeReject, 0)
	return c
}

func (p *ViewChangeRejectCache) Get() *ViewChangeReject {
	var t *ViewChangeReject
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ViewChangeReject{}
	}
	return t
}
func (p *ViewChangeRejectCache) Put(t *ViewChangeReject) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ViewChangeReject) Marshal(wire io.Writer) {
	var b [24]byte
	var bs []byte
	bs = b[:24]
	tmp32 := t.PilotId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.ViewId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.OldView.ViewId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.OldView.PilotId
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = t.OldView.ReplicaId
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	tmp32 = t.NewViewId
	bs[20] = byte(tmp32)
	bs[21] = byte(tmp32 >> 8)
	bs[22] = byte(tmp32 >> 16)
	bs[23] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ViewChangeReject) Unmarshal(wire io.Reader) error {
	var b [24]byte
	var bs []byte
	bs = b[:24]
	if _, err := io.ReadAtLeast(wire, bs, 24); err != nil {
		return err
	}
	t.PilotId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ViewId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.OldView.ViewId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.OldView.PilotId = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.OldView.ReplicaId = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	t.NewViewId = int32((uint32(bs[20]) | (uint32(bs[21]) << 8) | (uint32(bs[22]) << 16) | (uint32(bs[23]) << 24)))
	return nil
}

func (t *AcceptView) BinarySize() (nbytes int, sizeKnown bool) {
	return 28, true
}

type AcceptViewCache struct {
	mu	sync.Mutex
	cache	[]*AcceptView
}

func NewAcceptViewCache() *AcceptViewCache {
	c := &AcceptViewCache{}
	c.cache = make([]*AcceptView, 0)
	return c
}

func (p *AcceptViewCache) Get() *AcceptView {
	var t *AcceptView
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AcceptView{}
	}
	return t
}
func (p *AcceptViewCache) Put(t *AcceptView) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *AcceptView) Marshal(wire io.Writer) {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	tmp32 := t.ViewManagerId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.PilotId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.LatestCommittedInstance
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.LatestInstance
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = t.NewView.ViewId
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	tmp32 = t.NewView.PilotId
	bs[20] = byte(tmp32)
	bs[21] = byte(tmp32 >> 8)
	bs[22] = byte(tmp32 >> 16)
	bs[23] = byte(tmp32 >> 24)
	tmp32 = t.NewView.ReplicaId
	bs[24] = byte(tmp32)
	bs[25] = byte(tmp32 >> 8)
	bs[26] = byte(tmp32 >> 16)
	bs[27] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *AcceptView) Unmarshal(wire io.Reader) error {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	if _, err := io.ReadAtLeast(wire, bs, 28); err != nil {
		return err
	}
	t.ViewManagerId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.PilotId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.LatestCommittedInstance = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.LatestInstance = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.NewView.ViewId = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	t.NewView.PilotId = int32((uint32(bs[20]) | (uint32(bs[21]) << 8) | (uint32(bs[22]) << 16) | (uint32(bs[23]) << 24)))
	t.NewView.ReplicaId = int32((uint32(bs[24]) | (uint32(bs[25]) << 8) | (uint32(bs[26]) << 16) | (uint32(bs[27]) << 24)))
	return nil
}
