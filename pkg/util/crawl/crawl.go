package crawl

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/temoto/robotstxt"

	"github.com/glidea/zenfeed/pkg/util/text_convert"
)

type Crawler interface {
	Markdown(ctx context.Context, u string) ([]byte, error)
}

const (
	httpClientTimeout = 45 * time.Second
	maxPageBodyBytes  = 16 << 20
	maxRobotsBytes    = 1 << 20
	maxRedirects      = 10
)

type local struct {
	hc *http.Client

	robotsDataCache sync.Map
}

type ipResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type dialContextFunc func(ctx context.Context, network, address string) (net.Conn, error)

func NewLocal() Crawler {
	return &local{
		hc: newSafeHTTPClient(net.DefaultResolver, (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext),
	}
}

func newSafeHTTPClient(resolver ipResolver, dialContext dialContextFunc) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// An environment-configured proxy could resolve and dial the destination
	// outside the checks performed by safeDialContext.
	transport.Proxy = nil
	transport.DialContext = newSafeDialContext(resolver, dialContext)

	return &http.Client{
		Timeout:   httpClientTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return errors.Errorf("stopped after %d redirects", maxRedirects)
			}
			return validateHTTPURL(req.URL)
		},
	}
}

func newSafeDialContext(resolver ipResolver, dialContext dialContextFunc) dialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.Wrapf(err, "split dial address %q", address)
		}

		if err := validateHostname(host); err != nil {
			return nil, err
		}

		addresses, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrapf(err, "resolve host %q", host)
		}
		if len(addresses) == 0 {
			return nil, errors.Errorf("host %q resolved to no addresses", host)
		}

		// Validate the complete answer before dialing any address. Besides
		// rejecting mixed public/private answers, dialing the numeric address
		// below prevents a second DNS lookup and DNS-rebinding race.
		for _, resolved := range addresses {
			if err := validatePublicIP(resolved.IP); err != nil {
				return nil, errors.Wrapf(err, "host %q resolved to an unsafe address", host)
			}
		}

		var lastErr error
		for _, resolved := range addresses {
			ipHost := resolved.IP.String()
			if resolved.Zone != "" {
				ipHost += "%" + resolved.Zone
			}
			conn, err := dialContext(ctx, network, net.JoinHostPort(ipHost, port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}

		return nil, errors.Wrapf(lastErr, "dial validated addresses for %q", address)
	}
}

func (c *local) Markdown(ctx context.Context, u string) ([]byte, error) {
	pageURL, err := url.Parse(u)
	if err != nil {
		return nil, errors.Wrapf(err, "parse url %s", u)
	}
	if err := validateHTTPURL(pageURL); err != nil {
		return nil, err
	}

	for redirects := 0; ; redirects++ {
		// Robots rules are origin- and path-specific, so re-check them after
		// every redirect rather than applying only the initial URL's rules.
		if err := c.checkAllowedURL(ctx, pageURL); err != nil {
			return nil, errors.Wrapf(err, "check robots.txt for %s", pageURL.String())
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL.String(), nil)
		if err != nil {
			return nil, errors.Wrapf(err, "create request for %s", pageURL.String())
		}
		req.Header.Set("User-Agent", userAgent)

		resp, err := c.doWithoutRedirects(req)
		if err != nil {
			return nil, errors.Wrapf(err, "fetch %s", pageURL.String())
		}

		redirectURL, redirected, err := nextRedirectURL(resp, pageURL)
		if redirected {
			_ = resp.Body.Close()
			if err != nil {
				return nil, errors.Wrapf(err, "follow redirect from %s", pageURL.String())
			}
			if redirects >= maxRedirects {
				return nil, errors.Errorf("stopped after %d redirects", maxRedirects)
			}
			pageURL = redirectURL
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, errors.Errorf("received non-200 status code %d from %s", resp.StatusCode, pageURL.String())
		}

		bodyBytes, err := readLimited(resp.Body, maxPageBodyBytes)
		_ = resp.Body.Close()
		if err != nil {
			return nil, errors.Wrapf(err, "read body from %s", pageURL.String())
		}

		mdBytes, err := textconvert.HTMLToMarkdown(bodyBytes)
		if err != nil {
			return nil, errors.Wrap(err, "convert html to markdown")
		}

		return mdBytes, nil
	}
}

const userAgent = "ZenFeed"

func (c *local) checkAllowed(ctx context.Context, u string) error {
	parsedURL, err := url.Parse(u)
	if err != nil {
		return errors.Wrapf(err, "parse url %s", u)
	}
	if err := validateHTTPURL(parsedURL); err != nil {
		return err
	}
	return c.checkAllowedURL(ctx, parsedURL)
}

func (c *local) checkAllowedURL(ctx context.Context, parsedURL *url.URL) error {
	d, err := c.getRobotsData(ctx, parsedURL)
	if err != nil {
		return errors.Wrapf(err, "check robots.txt for %s", parsedURL.Host)
	}
	if !d.TestAgent(parsedURL.Path, userAgent) {
		return errors.Errorf("disallowed by robots.txt for %s", parsedURL.String())
	}

	return nil
}

func validateHTTPURL(parsedURL *url.URL) error {
	if err := validateHTTPURLSyntax(parsedURL); err != nil {
		return err
	}
	if err := validateHostname(parsedURL.Hostname()); err != nil {
		return err
	}

	return nil
}

func validateHTTPURLSyntax(parsedURL *url.URL) error {
	if parsedURL == nil || !parsedURL.IsAbs() || parsedURL.Host == "" ||
		(!strings.EqualFold(parsedURL.Scheme, "http") && !strings.EqualFold(parsedURL.Scheme, "https")) {
		return errors.New("URL must be an absolute HTTP or HTTPS URL")
	}
	if parsedURL.User != nil {
		return errors.New("URL credentials are not allowed")
	}
	if parsedURL.Hostname() == "" {
		return errors.New("URL must include a hostname")
	}
	// Make the accepted case-insensitive schemes usable by net/http too.
	parsedURL.Scheme = strings.ToLower(parsedURL.Scheme)
	return nil
}

func (c *local) doWithoutRedirects(req *http.Request) (*http.Response, error) {
	client := *c.hc
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if c.hc.CheckRedirect != nil {
			if err := c.hc.CheckRedirect(next, via); err != nil {
				return err
			}
		}
		return http.ErrUseLastResponse
	}
	return client.Do(req)
}

func nextRedirectURL(resp *http.Response, base *url.URL) (*url.URL, bool, error) {
	switch resp.StatusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
	default:
		return nil, false, nil
	}

	location := resp.Header.Get("Location")
	if location == "" {
		return nil, true, errors.New("redirect response is missing Location header")
	}
	next, err := base.Parse(location)
	if err != nil {
		return nil, true, errors.Wrap(err, "parse redirect URL")
	}
	if err := validateHTTPURL(next); err != nil {
		return nil, true, err
	}
	return next, true, nil
}

func validateHostname(host string) error {
	normalized := strings.TrimRight(strings.ToLower(host), ".")
	if normalized == "localhost" || strings.HasSuffix(normalized, ".localhost") {
		return errors.Errorf("localhost host %q is not allowed", host)
	}

	ipHost := host
	if zoneIndex := strings.LastIndexByte(ipHost, '%'); zoneIndex >= 0 {
		ipHost = ipHost[:zoneIndex]
	}
	ipHost = strings.TrimSuffix(ipHost, ".")
	if addr, err := netip.ParseAddr(ipHost); err == nil {
		return validatePublicAddr(addr)
	}

	return nil
}

func validatePublicIP(ip net.IP) error {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return errors.Errorf("invalid IP address %q", ip.String())
	}
	return validatePublicAddr(addr)
}

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/96"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("fec0::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

func validatePublicAddr(addr netip.Addr) error {
	addr = addr.Unmap()
	if !addr.IsValid() || !addr.IsGlobalUnicast() || addr.IsPrivate() ||
		addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsUnspecified() || addr.IsMulticast() {
		return errors.Errorf("IP address %q is not public", addr.String())
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(addr) {
			return errors.Errorf("IP address %q is not public", addr.String())
		}
	}

	return nil
}

// getRobotsData fetches and parses robots.txt for a given origin.
func (c *local) getRobotsData(ctx context.Context, pageURL *url.URL) (*robotstxt.RobotsData, error) {
	cacheKey := pageURL.Scheme + "://" + pageURL.Host
	// Check the cache.
	if data, found := c.robotsDataCache.Load(cacheKey); found {
		return data.(*robotstxt.RobotsData), nil
	}

	// Prepare the request.
	robotsURL := (&url.URL{
		Scheme: pageURL.Scheme,
		Host:   pageURL.Host,
		Path:   "/robots.txt",
	}).String()
	robotsParsedURL, err := url.Parse(robotsURL)
	if err != nil {
		return nil, errors.Wrapf(err, "parse url %s", robotsURL)
	}

	var resp *http.Response
	for redirects := 0; ; redirects++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsParsedURL.String(), nil)
		if err != nil {
			return nil, errors.Wrapf(err, "create request for %s", robotsParsedURL.String())
		}
		req.Header.Set("User-Agent", userAgent)

		resp, err = c.doWithoutRedirects(req)
		if err != nil {
			return nil, errors.Wrapf(err, "fetch %s", robotsParsedURL.String())
		}
		next, redirected, redirectErr := nextRedirectURL(resp, robotsParsedURL)
		if !redirected {
			break
		}
		_ = resp.Body.Close()
		if redirectErr != nil {
			return nil, errors.Wrapf(redirectErr, "follow redirect from %s", robotsParsedURL.String())
		}
		if redirects >= maxRedirects {
			return nil, errors.Errorf("stopped after %d redirects", maxRedirects)
		}
		robotsParsedURL = next
	}
	defer func() { _ = resp.Body.Close() }()

	// Parse the response.
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := readLimited(resp.Body, maxRobotsBytes)
		if err != nil {
			return nil, errors.Wrapf(err, "read robots.txt from %s", robotsURL)
		}
		data, err := robotstxt.FromBytes(body)
		if err != nil {
			return nil, errors.Wrapf(err, "parse robots.txt from %s", robotsURL)
		}
		c.robotsDataCache.Store(cacheKey, data)

		return data, nil

	case http.StatusNotFound:
		data := &robotstxt.RobotsData{}
		c.robotsDataCache.Store(cacheKey, data)

		return data, nil

	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, errors.Errorf("access to %s denied (status %d)", robotsURL, resp.StatusCode)
	default:
		return nil, errors.Errorf("unexpected status %d fetching %s", resp.StatusCode, robotsURL)
	}
}

type jina struct {
	hc    *http.Client
	token string
}

func NewJina(token string) Crawler {
	return &jina{
		hc: newSafeHTTPClient(net.DefaultResolver, (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext),

		// If token is empty, will not affect to use, but rate limit will be lower.
		// See https://jina.ai/api-dashboard/rate-limit.
		token: token,
	}
}

func (c *jina) Markdown(ctx context.Context, u string) ([]byte, error) {
	targetURL, err := url.Parse(u)
	if err != nil {
		return nil, errors.Wrapf(err, "parse url %s", u)
	}
	if err := validateHTTPURLSyntax(targetURL); err != nil {
		return nil, err
	}

	proxyURL := "https://r.jina.ai/" + u
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyURL, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "create request for %s", u)
	}

	req.Header.Set("X-Engine", "browser")
	req.Header.Set("X-Robots-Txt", userAgent)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "fetch %s", proxyURL)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("received non-200 status code %d from %s", resp.StatusCode, proxyURL)
	}

	mdBytes, err := readLimited(resp.Body, maxPageBodyBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "read body from %s", proxyURL)
	}

	return mdBytes, nil
}

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errors.Errorf("response exceeds %d bytes", limit)
	}

	return body, nil
}
