package dns

import (
	"context"
	"math"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	"minisky/pkg/state"
)

func TestResolverClampsLegacyTTLAfterRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "legacy-ttl")
	if err != nil {
		t.Fatal(err)
	}
	legacy := dnsMetadata{
		ZoneSeq: 1,
		Zones: map[string]persistedZone{
			"test:legacy": {
				Zone: &ManagedZone{Name: "legacy", DnsName: "Example.Test.", Visibility: "public"},
				RRSets: map[string]*ResourceRecordSet{
					"stale-negative-key": {
						Name: "Negative.Example.Test.", Type: "a", TTL: -1, Rrdatas: []string{"192.0.2.10"},
					},
					"stale-overflow-key": {
						Name: "Overflow.Example.Test.", Type: "A", TTL: int64(math.MaxUint32) + 1,
						Rrdatas: []string{"192.0.2.11"},
					},
				},
			},
		},
	}
	if err := store.Save(dnsStateEntry, legacy); err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(api, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	assertDNSAnswer(t, resolver.Addr(), "negative.example.test.", dnsmessage.TypeA, "192.0.2.10", 0)
	assertDNSAnswer(t, resolver.Addr(), "overflow.example.test.", dnsmessage.TypeA, "192.0.2.11", math.MaxUint32)
}

func TestAPIShutdownClosesConfiguredResolverIdempotently(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "resolver-lifecycle")
	t.Setenv("MINISKY_DNS_ADDR", "127.0.0.1:0")
	api := NewAPI()
	if api.resolver == nil {
		t.Fatal("configured resolver was not retained by API")
	}
	address := api.resolver.Addr()
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		t.Fatalf("resolver port was not released: %v", err)
	}
	_ = connection.Close()
}

func TestConfiguredResolverStartupFailureRemainsObservable(t *testing.T) {
	occupied, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "resolver-startup-failure")
	t.Setenv("MINISKY_DNS_ADDR", occupied.LocalAddr().String())

	api := NewAPI()
	if api.resolver != nil || api.initErr == nil {
		t.Fatalf("resolver=%v initErr=%v", api.resolver, api.initErr)
	}
	response := dnsRequest(api, http.MethodGet, "/dns/v1/projects/test/managedZones", "")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestResolverServesUpdatesDeletesAndRestartedRecords(t *testing.T) {
	store, err := state.New(t.TempDir(), "dns-resolver")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	createResolverFixture(t, api)
	resolver, err := NewResolver(api, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()

	assertDNSAnswer(t, resolver.Addr(), "www.example.test.", dnsmessage.TypeA, "192.0.2.10", 60)
	assertDNSAnswer(t, resolver.Addr(), "v6.example.test.", dnsmessage.TypeAAAA, "2001:db8::10", 120)
	assertDNSAnswer(t, resolver.Addr(), "alias.example.test.", dnsmessage.TypeCNAME, "www.example.test.", 30)
	assertDNSAnswer(t, resolver.Addr(), "alias.example.test.", dnsmessage.TypeA, "www.example.test.", 30)

	update := dnsRequest(api, "PUT", "/dns/v1/projects/test/managedZones/example/rrsets/www.example.test./A",
		`{"name":"www.example.test.","type":"A","ttl":15,"rrdatas":["192.0.2.20"]}`)
	if update.Code != 200 {
		t.Fatalf("update status = %d, body = %s", update.Code, update.Body.String())
	}
	assertDNSAnswer(t, resolver.Addr(), "www.example.test.", dnsmessage.TypeA, "192.0.2.20", 15)

	deleted := dnsRequest(api, "DELETE", "/dns/v1/projects/test/managedZones/example/rrsets/alias.example.test./CNAME", "")
	if deleted.Code != 204 {
		t.Fatalf("delete status = %d, body = %s", deleted.Code, deleted.Body.String())
	}
	assertDNSRCode(t, resolver.Addr(), "alias.example.test.", dnsmessage.TypeCNAME, dnsmessage.RCodeNameError)

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restartedResolver, err := NewResolver(restarted, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer restartedResolver.Close()
	assertDNSAnswer(t, restartedResolver.Addr(), "www.example.test.", dnsmessage.TypeA, "192.0.2.20", 15)
}

func TestResolverRejectsNonLoopbackBinding(t *testing.T) {
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewResolver(api, "0.0.0.0:1053"); err == nil {
		t.Fatal("non-loopback resolver binding was accepted")
	}
	if _, err := NewResolver(api, "127.0.0.1:53"); err == nil {
		t.Fatal("privileged resolver port was accepted")
	}
}

func TestResolverSelectsProjectVisibilityAndLongestSuffixDeterministically(t *testing.T) {
	t.Setenv("MINISKY_DNS_PROJECT", "selected")
	t.Setenv("MINISKY_DNS_VISIBILITY", "public")
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct{ method, path, body string }{
		{"POST", "/dns/v1/projects/selected/managedZones", `{"name":"broad","dnsName":"example.test.","visibility":"public"}`},
		{"POST", "/dns/v1/projects/selected/managedZones/broad/rrsets", `{"name":"www.sub.example.test.","type":"A","ttl":30,"rrdatas":["192.0.2.1"]}`},
		{"POST", "/dns/v1/projects/selected/managedZones", `{"name":"specific","dnsName":"sub.example.test.","visibility":"public"}`},
		{"POST", "/dns/v1/projects/selected/managedZones/specific/rrsets", `{"name":"www.sub.example.test.","type":"A","ttl":60,"rrdatas":["192.0.2.2"]}`},
		{"POST", "/dns/v1/projects/other/managedZones", `{"name":"specific","dnsName":"sub.example.test.","visibility":"public"}`},
		{"POST", "/dns/v1/projects/other/managedZones/specific/rrsets", `{"name":"www.sub.example.test.","type":"A","ttl":90,"rrdatas":["192.0.2.3"]}`},
	} {
		response := dnsRequest(api, fixture.method, fixture.path, fixture.body)
		if response.Code != http.StatusOK {
			t.Fatalf("%s %s = %d %s", fixture.method, fixture.path, response.Code, response.Body.String())
		}
	}
	resolver, err := NewResolver(api, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	assertDNSAnswer(t, resolver.Addr(), "www.sub.example.test.", dnsmessage.TypeA, "192.0.2.2", 60)
}

func createResolverFixture(t *testing.T, api *API) {
	t.Helper()
	for _, request := range []struct {
		path string
		body string
	}{
		{"/dns/v1/projects/test/managedZones", `{"name":"example","dnsName":"example.test."}`},
		{"/dns/v1/projects/test/managedZones/example/rrsets", `{"name":"www.example.test.","type":"A","ttl":60,"rrdatas":["192.0.2.10"]}`},
		{"/dns/v1/projects/test/managedZones/example/rrsets", `{"name":"v6.example.test.","type":"AAAA","ttl":120,"rrdatas":["2001:db8::10"]}`},
		{"/dns/v1/projects/test/managedZones/example/rrsets", `{"name":"alias.example.test.","type":"CNAME","ttl":30,"rrdatas":["www.example.test."]}`},
	} {
		response := dnsRequest(api, "POST", request.path, request.body)
		if response.Code != 200 {
			t.Fatalf("fixture status = %d, body = %s", response.Code, response.Body.String())
		}
	}
}

func assertDNSAnswer(t *testing.T, address, name string, queryType dnsmessage.Type, value string, ttl uint32) {
	t.Helper()
	response := dnsQuery(t, address, name, queryType)
	if response.Header.RCode != dnsmessage.RCodeSuccess || len(response.Answers) != 1 {
		t.Fatalf("response = %#v", response)
	}
	answer := response.Answers[0]
	if answer.Header.TTL != ttl {
		t.Fatalf("TTL = %d, want %d", answer.Header.TTL, ttl)
	}
	var got string
	switch body := answer.Body.(type) {
	case *dnsmessage.AResource:
		got = net.IP(body.A[:]).String()
	case *dnsmessage.AAAAResource:
		got = net.IP(body.AAAA[:]).String()
	case *dnsmessage.CNAMEResource:
		got = body.CNAME.String()
	default:
		t.Fatalf("unexpected answer body %T", answer.Body)
	}
	if got != value {
		t.Fatalf("answer = %q, want %q", got, value)
	}
}

func assertDNSRCode(t *testing.T, address, name string, queryType dnsmessage.Type, code dnsmessage.RCode) {
	t.Helper()
	response := dnsQuery(t, address, name, queryType)
	if response.Header.RCode != code {
		t.Fatalf("RCode = %v, want %v", response.Header.RCode, code)
	}
}

func dnsQuery(t *testing.T, address, name string, queryType dnsmessage.Type) dnsmessage.Message {
	t.Helper()
	dnsName, err := dnsmessage.NewName(name)
	if err != nil {
		t.Fatal(err)
	}
	query := dnsmessage.Message{
		Header: dnsmessage.Header{ID: 42, RecursionDesired: true},
		Questions: []dnsmessage.Question{{
			Name: dnsName, Type: queryType, Class: dnsmessage.ClassINET,
		}},
	}
	packet, err := query.Pack()
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTimeout("udp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	if _, err := connection.Write(packet); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1500)
	size, err := connection.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	var response dnsmessage.Message
	if err := response.Unpack(buffer[:size]); err != nil {
		t.Fatal(err)
	}
	return response
}
