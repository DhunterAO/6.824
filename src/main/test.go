package main

import (
	"fmt"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

type UrlsWithDepth struct {
	urls []string
	depth int
}

func worker(url string, depth int, ch chan UrlsWithDepth, fetcher Fetcher) {
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		ch <- UrlsWithDepth{}
	} else {
		fmt.Printf("found: %s %q\n", url, body)
		ch <- UrlsWithDepth{urls: urls, depth: depth}
	}
}

func coordinator(ch chan UrlsWithDepth, fetcher Fetcher) {
	n := 1
	fetched := make(map[string]bool)
	for urlsWithDepth := range ch {
		urls := urlsWithDepth.urls
		depth := urlsWithDepth.depth
		for _, u := range urls {
			if depth > 0 {
				if fetched[u] == false {
					// fetched[u] = true
					n += 1
					go worker(u, depth - 1, ch, fetcher)
				}
			}
		}
		n -= 1
		if n == 0 {
			break
		}
	}
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	ch := make(chan UrlsWithDepth)
	go func() {
		ch <- UrlsWithDepth{urls: []string{url}, depth: depth}
	}()
	coordinator(ch, fetcher)
	return
}

func main() {
	Crawl("https://golang.org/", 4, fetcher)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
