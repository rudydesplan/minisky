package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	zoneName = "phase16-zone"
	recordIP = "192.0.2.16"
)

var dnsLabelPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "phase 16 Cloud DNS smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	mode := strings.TrimSpace(os.Getenv("MINISKY_PHASE16_DNS_MODE"))
	gateway := strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/")
	project := env("MINISKY_PROJECT_ID", "phase16-project")
	dnsAddr := strings.TrimSpace(os.Getenv("MINISKY_DNS_ADDR"))
	if err := validateInputs(gateway, project, dnsAddr); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch mode {
	case "seed":
		return seed(ctx, gateway, project)
	case "verify":
		return verify(ctx, gateway, project, dnsAddr)
	case "cleanup":
		return cleanup(ctx, gateway, project)
	case "verify-cleanup":
		return verifyCleanup(ctx, gateway, project, dnsAddr)
	default:
		return errors.New("MINISKY_PHASE16_DNS_MODE must be seed, verify, cleanup, or verify-cleanup")
	}
}

func dnsService(ctx context.Context, gateway string) (*dns.Service, error) {
	return dns.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(strings.TrimRight(gateway, "/")+"/_minisky/dns/"),
	)
}

func seed(ctx context.Context, gateway, project string) error {
	service, err := dnsService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create DNS client: %w", err)
	}
	zone := &dns.ManagedZone{
		Name:       zoneName,
		DnsName:    dnsNameForProject(project),
		Visibility: "public",
	}
	created, err := service.ManagedZones.Create(project, zone).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create managed zone with generated client: %w", err)
	}
	if err := validateManagedZone(created, project); err != nil {
		return err
	}
	fetched, err := service.ManagedZones.Get(project, zoneName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get created managed zone with generated client: %w", err)
	}
	if err := validateManagedZone(fetched, project); err != nil {
		return err
	}
	record := expectedRecord(project)
	createdRecord, err := service.ResourceRecordSets.Create(project, zoneName, record).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create resource record set with generated client: %w", err)
	}
	if err := validateRecord(createdRecord, project); err != nil {
		return err
	}
	fmt.Printf("seeded Cloud DNS zone=%s record=%s\n", zoneName, record.Name)
	return nil
}

func verify(ctx context.Context, gateway, project, dnsAddr string) error {
	service, err := dnsService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create DNS client: %w", err)
	}
	zone, err := service.ManagedZones.Get(project, zoneName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get persisted managed zone: %w", err)
	}
	if err := validateManagedZone(zone, project); err != nil {
		return err
	}
	zones, err := service.ManagedZones.List(project).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list persisted managed zones: %w", err)
	}
	if len(zones.ManagedZones) != 1 {
		return fmt.Errorf("listed managed zones=%d want=1", len(zones.ManagedZones))
	}
	if err := validateManagedZone(zones.ManagedZones[0], project); err != nil {
		return err
	}
	recordName := expectedRecord(project).Name
	record, err := service.ResourceRecordSets.Get(project, zoneName, strings.ToUpper(recordName), "a").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get persisted resource record set: %w", err)
	}
	if err := validateRecord(record, project); err != nil {
		return err
	}
	records, err := service.ResourceRecordSets.List(project, zoneName).
		Name(recordName).Type("A").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list filtered resource record sets: %w", err)
	}
	if len(records.Rrsets) != 1 {
		return fmt.Errorf("listed filtered resource record sets=%d want=1", len(records.Rrsets))
	}
	if err := validateRecord(records.Rrsets[0], project); err != nil {
		return err
	}
	if err := assertUDPRecord(ctx, dnsAddr, recordName, true); err != nil {
		return err
	}
	fmt.Printf("verified persisted Cloud DNS zone=%s record=%s over generated SDK and UDP\n", zoneName, recordName)
	return nil
}

func cleanup(ctx context.Context, gateway, project string) error {
	service, err := dnsService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create DNS client: %w", err)
	}
	recordName := expectedRecord(project).Name
	if _, err := service.ResourceRecordSets.Delete(project, zoneName, recordName, "A").Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete resource record set with generated client: %w", err)
	}
	if err := confirmRecordMissing(ctx, service, project, recordName); err != nil {
		return err
	}
	if err := service.ManagedZones.Delete(project, zoneName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete managed zone with generated client: %w", err)
	}
	if err := confirmZoneMissing(ctx, service, project); err != nil {
		return err
	}
	fmt.Printf("deleted Cloud DNS record=%s zone=%s and confirmed generated GET 404\n", recordName, zoneName)
	return nil
}

func verifyCleanup(ctx context.Context, gateway, project, dnsAddr string) error {
	service, err := dnsService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create DNS client: %w", err)
	}
	if err := confirmZoneMissing(ctx, service, project); err != nil {
		return err
	}
	recordName := expectedRecord(project).Name
	if err := assertUDPRecord(ctx, dnsAddr, recordName, false); err != nil {
		return err
	}
	fmt.Printf("verified Cloud DNS cleanup after restart zone=%s remains 404 and UDP returns NXDOMAIN\n", zoneName)
	return nil
}

func confirmRecordMissing(ctx context.Context, service *dns.Service, project, recordName string) error {
	_, err := service.ResourceRecordSets.Get(project, zoneName, recordName, "A").Context(ctx).Do()
	return requireNotFound(err, "deleted resource record set")
}

func confirmZoneMissing(ctx context.Context, service *dns.Service, project string) error {
	_, err := service.ManagedZones.Get(project, zoneName).Context(ctx).Do()
	return requireNotFound(err, "deleted managed zone")
}

func requireNotFound(err error, resource string) error {
	if err == nil {
		return fmt.Errorf("%s still exists", resource)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != 404 {
		return fmt.Errorf("get %s: %w", resource, err)
	}
	return nil
}

func validateManagedZone(zone *dns.ManagedZone, project string) error {
	if zone == nil || zone.Name != zoneName || zone.DnsName != dnsNameForProject(project) ||
		zone.Visibility != "public" {
		return fmt.Errorf("unexpected managed zone: %#v", zone)
	}
	return nil
}

func expectedRecord(project string) *dns.ResourceRecordSet {
	return &dns.ResourceRecordSet{
		Name:    "app." + dnsNameForProject(project),
		Type:    "A",
		Ttl:     60,
		Rrdatas: []string{recordIP},
	}
}

func validateRecord(record *dns.ResourceRecordSet, project string) error {
	expected := expectedRecord(project)
	if record == nil || record.Name != expected.Name || record.Type != expected.Type ||
		record.Ttl != expected.Ttl || len(record.Rrdatas) != 1 || record.Rrdatas[0] != recordIP {
		return fmt.Errorf("unexpected resource record set: %#v", record)
	}
	return nil
}

func assertUDPRecord(ctx context.Context, address, recordName string, shouldExist bool) error {
	name, err := dnsmessage.NewName(recordName)
	if err != nil {
		return fmt.Errorf("build DNS query name: %w", err)
	}
	query := dnsmessage.Message{
		Header: dnsmessage.Header{ID: 16},
		Questions: []dnsmessage.Question{{
			Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET,
		}},
	}
	packet, err := query.Pack()
	if err != nil {
		return fmt.Errorf("pack DNS query: %w", err)
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "udp", address)
	if err != nil {
		return fmt.Errorf("dial DNS resolver: %w", err)
	}
	defer connection.Close()
	deadline := time.Now().Add(3 * time.Second)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return err
	}
	if _, err := connection.Write(packet); err != nil {
		return fmt.Errorf("write DNS query: %w", err)
	}
	buffer := make([]byte, 1500)
	size, err := connection.Read(buffer)
	if err != nil {
		return fmt.Errorf("read DNS response: %w", err)
	}
	var response dnsmessage.Message
	if err := response.Unpack(buffer[:size]); err != nil {
		return fmt.Errorf("unpack DNS response: %w", err)
	}
	if !response.Header.Response || response.Header.Truncated {
		return fmt.Errorf("invalid DNS response header: %#v", response.Header)
	}
	if !shouldExist {
		if response.Header.RCode != dnsmessage.RCodeNameError || len(response.Answers) != 0 {
			return fmt.Errorf("cleanup DNS response rcode=%v answers=%d", response.Header.RCode, len(response.Answers))
		}
		return nil
	}
	if response.Header.RCode != dnsmessage.RCodeSuccess || len(response.Answers) != 1 {
		return fmt.Errorf("DNS response rcode=%v answers=%d", response.Header.RCode, len(response.Answers))
	}
	answer := response.Answers[0]
	body, ok := answer.Body.(*dnsmessage.AResource)
	if !ok || answer.Header.Name.String() != recordName || answer.Header.Type != dnsmessage.TypeA ||
		answer.Header.Class != dnsmessage.ClassINET || answer.Header.TTL != 60 ||
		net.IP(body.A[:]).String() != recordIP {
		return fmt.Errorf("unexpected DNS answer: %#v", answer)
	}
	return nil
}

func validateInputs(gateway, project, dnsAddr string) error {
	parsed, err := url.Parse(gateway)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("MINISKY_ENDPOINT must be an absolute HTTP(S) URL without query or fragment")
	}
	if strings.Contains(project, "/") || !dnsLabelPattern.MatchString(project) {
		return errors.New("MINISKY_PROJECT_ID must be a lowercase DNS label and nonempty path segment")
	}
	host, portText, err := net.SplitHostPort(dnsAddr)
	if err != nil {
		return fmt.Errorf("MINISKY_DNS_ADDR must be a host:port address: %w", err)
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.IsLoopback() {
		return errors.New("MINISKY_DNS_ADDR must use a loopback IP")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1024 || port > 65535 {
		return errors.New("MINISKY_DNS_ADDR must use a port from 1024 through 65535")
	}
	if _, err := dnsmessage.NewName(dnsNameForProject(project)); err != nil {
		return fmt.Errorf("derived DNS name is invalid: %w", err)
	}
	if address := net.ParseIP(recordIP); address == nil || address.To4() == nil {
		return errors.New("configured A record IP is invalid")
	}
	return nil
}

func dnsNameForProject(project string) string {
	return project + ".example.test."
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
