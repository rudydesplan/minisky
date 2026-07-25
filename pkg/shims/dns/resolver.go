package dns

import (
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/dns/dnsmessage"
)

// Resolver is a loopback-only UDP DNS data plane backed directly by managed
// zone record state. It intentionally supports only A, AAAA, and CNAME.
type Resolver struct {
	api        *API
	connection *net.UDPConn
	project    string
	visibility string
	closeOnce  sync.Once
}

// NewResolver binds a non-privileged loopback UDP address. Port zero requests
// an ephemeral port and is useful for collision-free tests.
func NewResolver(api *API, address string) (*Resolver, error) {
	if api == nil {
		return nil, fmt.Errorf("DNS API is required")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid resolver address: %w", err)
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("DNS resolver must bind to a loopback address")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("invalid DNS resolver port")
	}
	if port > 0 && port < 1024 {
		return nil, fmt.Errorf("DNS resolver port must be non-privileged")
	}
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	connection, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return nil, err
	}
	visibility := strings.ToLower(strings.TrimSpace(os.Getenv("MINISKY_DNS_VISIBILITY")))
	if visibility == "" {
		visibility = "public"
	}
	resolver := &Resolver{
		api: api, connection: connection,
		project: strings.TrimSpace(os.Getenv("MINISKY_DNS_PROJECT")), visibility: visibility,
	}
	go resolver.serve()
	return resolver, nil
}

func (r *Resolver) Addr() string {
	return r.connection.LocalAddr().String()
}

func (r *Resolver) Close() error {
	var err error
	r.closeOnce.Do(func() { err = r.connection.Close() })
	return err
}

func (r *Resolver) serve() {
	buffer := make([]byte, 4096)
	for {
		size, peer, err := r.connection.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		packet := append([]byte(nil), buffer[:size]...)
		go r.respond(peer, packet)
	}
}

func (r *Resolver) respond(peer *net.UDPAddr, packet []byte) {
	var query dnsmessage.Message
	if err := query.Unpack(packet); err != nil || len(query.Questions) != 1 {
		return
	}
	question := query.Questions[0]
	answers, nameExists := r.lookup(question)
	code := dnsmessage.RCodeSuccess
	if !nameExists {
		code = dnsmessage.RCodeNameError
	}
	response := dnsmessage.Message{
		Header: dnsmessage.Header{
			ID:                 query.Header.ID,
			Response:           true,
			Authoritative:      true,
			RecursionDesired:   query.Header.RecursionDesired,
			RecursionAvailable: false,
			RCode:              code,
		},
		Questions: query.Questions,
		Answers:   answers,
	}
	encoded, err := response.Pack()
	if err != nil {
		return
	}
	_, _ = r.connection.WriteToUDP(encoded, peer)
}

func (r *Resolver) lookup(question dnsmessage.Question) ([]dnsmessage.Resource, bool) {
	name := strings.ToLower(question.Name.String())
	requestedType := map[dnsmessage.Type]string{
		dnsmessage.TypeA:     "A",
		dnsmessage.TypeAAAA:  "AAAA",
		dnsmessage.TypeCNAME: "CNAME",
	}[question.Type]
	r.api.mu.RLock()
	defer r.api.mu.RUnlock()

	type candidateZone struct {
		key   string
		store *zoneStore
	}
	var candidates []candidateZone
	for key, zone := range r.api.zones {
		if zone == nil || zone.zone == nil || !strings.HasSuffix(name, strings.ToLower(zone.zone.DnsName)) {
			continue
		}
		project := strings.SplitN(key, ":", 2)[0]
		visibility := strings.ToLower(strings.TrimSpace(zone.zone.Visibility))
		if visibility == "" {
			visibility = "public"
		}
		if r.project != "" && project != r.project {
			continue
		}
		if visibility != r.visibility {
			continue
		}
		candidates = append(candidates, candidateZone{key: key, store: zone})
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i].store.zone.DnsName, candidates[j].store.zone.DnsName
		if len(left) != len(right) {
			return len(left) > len(right)
		}
		return candidates[i].key < candidates[j].key
	})
	if len(candidates) == 0 {
		return nil, false
	}
	zone := candidates[0].store
	nameExists := false
	for _, rrset := range zone.rrsets {
		if rrset == nil || strings.ToLower(rrset.Name) != name {
			continue
		}
		nameExists = true
		answerType := requestedType
		headerType := question.Type
		if !strings.EqualFold(rrset.Type, answerType) {
			if (question.Type != dnsmessage.TypeA && question.Type != dnsmessage.TypeAAAA) ||
				!strings.EqualFold(rrset.Type, "CNAME") {
				continue
			}
			answerType = "CNAME"
			headerType = dnsmessage.TypeCNAME
		}
		header := dnsmessage.ResourceHeader{
			Name:  question.Name,
			Type:  headerType,
			Class: dnsmessage.ClassINET,
			TTL:   uint32(max(rrset.TTL, 0)),
		}
		answers := make([]dnsmessage.Resource, 0, len(rrset.Rrdatas))
		for _, value := range rrset.Rrdatas {
			bodyType := question.Type
			if answerType == "CNAME" {
				bodyType = dnsmessage.TypeCNAME
			}
			body, ok := dnsBody(bodyType, value)
			if ok {
				answers = append(answers, dnsmessage.Resource{Header: header, Body: body})
			}
		}
		return answers, nameExists
	}
	return nil, nameExists
}

func dnsBody(recordType dnsmessage.Type, value string) (dnsmessage.ResourceBody, bool) {
	switch recordType {
	case dnsmessage.TypeA:
		address, err := netip.ParseAddr(strings.TrimSpace(value))
		if err != nil || !address.Is4() {
			return nil, false
		}
		return &dnsmessage.AResource{A: address.As4()}, true
	case dnsmessage.TypeAAAA:
		address, err := netip.ParseAddr(strings.TrimSpace(value))
		if err != nil || !address.Is6() {
			return nil, false
		}
		return &dnsmessage.AAAAResource{AAAA: address.As16()}, true
	case dnsmessage.TypeCNAME:
		name, err := dnsmessage.NewName(ensureFQDN(strings.TrimSpace(value)))
		if err != nil {
			return nil, false
		}
		return &dnsmessage.CNAMEResource{CNAME: name}, true
	default:
		return nil, false
	}
}

func ensureFQDN(name string) string {
	if !strings.HasSuffix(name, ".") {
		return name + "."
	}
	return name
}

func startConfiguredResolver(api *API) {
	address := strings.TrimSpace(os.Getenv("MINISKY_DNS_ADDR"))
	if address == "" {
		return
	}
	resolver, err := NewResolver(api, address)
	if err != nil {
		log.Printf("[Shim: Cloud DNS] resolver disabled: %v", err)
		return
	}
	log.Printf("[Shim: Cloud DNS] loopback resolver listening on %s", resolver.Addr())
}
