// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package issues

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-github/github"
)

func TestPost(t *testing.T) {
	const (
		expOwner     = "cockroachdb"
		expRepo      = "cockroach"
		expAssignee  = "hodor"
		expMilestone = 2
		envTags      = "deadlock"
		envGoFlags   = "race"
		sha          = "abcd123"
		serverURL    = "https://teamcity.example.com"
		buildID      = 8008135
		issueID      = 1337
		issueNumber  = 30
	)

	for key, value := range map[string]string{
		teamcityVCSNumberEnv: sha,
		teamcityServerURLEnv: serverURL,
		teamcityBuildIDEnv:   strconv.Itoa(buildID),
		tagsEnv:              envTags,
		goFlagsEnv:           envGoFlags,
	} {
		if val, ok := os.LookupEnv(key); ok {
			defer func() {
				if err := os.Setenv(key, val); err != nil {
					t.Error(err)
				}
			}()
		} else {
			defer func() {
				if err := os.Unsetenv(key); err != nil {
					t.Error(err)
				}
			}()
		}

		if err := os.Setenv(key, value); err != nil {
			t.Fatal(err)
		}
	}

	parameters := "```\n" + strings.Join([]string{
		tagsEnv + "=" + envTags,
		goFlagsEnv + "=" + envGoFlags,
	}, "\n") + "\n```"

	testCases := []struct {
		name        string
		packageName string
		testName    string
		message     string
		author      string
	}{
		{
			name:        "failure",
			packageName: "github.com/cockroachdb/cockroach/pkg/storage",
			testName:    "TestReplicateQueueRebalance",
			message: "	<autogenerated>:12: storage/replicate_queue_test.go:103, condition failed to evaluate within 45s: not balanced: [10 1 10 1 8]",
			author: "bran",
		},
		{
			name:        "fatal",
			packageName: "github.com/cockroachdb/cockroach/pkg/storage",
			testName:    "TestGossipHandlesReplacedNode",
			message:     "F170517 07:33:43.763059 69575 storage/replica.go:1360  [n3,s3,r1/3:/M{in-ax}] on-disk and in-memory state diverged:",
			author:      "bran",
		},
	}

	for _, c := range testCases {
		for _, foundIssue := range []bool{true, false} {
			name := c.name
			if foundIssue {
				name = name + "-existing-issue"
			}
			t.Run(name, func(t *testing.T) {
				reString := fmt.Sprintf(`(?s)\ASHA: https://github.com/cockroachdb/cockroach/commits/%s

Parameters:
%s

To repro, try:

`+"```"+`
# Don't forget to check out a clean suitable branch and experiment with the
# stress invocation until the desired results present themselves. For example,
# using stress instead of stressrace and passing the '-p' stressflag which
# controls concurrency.
`+regexp.QuoteMeta(`./scripts/gceworker.sh start && ./scripts/gceworker.sh mosh
cd ~/go/src/github.com/cockroachdb/cockroach && \
stdbuf -oL -eL \
make stressrace TESTS=%s PKG=%s TESTTIMEOUT=5m STRESSFLAGS='-maxtime 20m -timeout 10m' 2>&1 | tee /tmp/stress.log`)+`
`+"```"+`

Failed test: %s`,
					regexp.QuoteMeta(sha),
					regexp.QuoteMeta(parameters),
					c.testName,
					c.packageName,
					regexp.QuoteMeta(fmt.Sprintf("%s/viewLog.html?buildId=%d&tab=buildLog", serverURL, buildID)),
				)

				issueBodyRe, err := regexp.Compile(
					fmt.Sprintf(reString+`

.*
%s
`, regexp.QuoteMeta(c.message)),
				)
				if err != nil {
					t.Fatal(err)
				}
				commentBodyRe, err := regexp.Compile(reString)
				if err != nil {
					t.Fatal(err)
				}

				issueCount := 0
				commentCount := 0

				p := &poster{}

				p.createIssue = func(_ context.Context, owner string, repo string,
					issue *github.IssueRequest) (*github.Issue, *github.Response, error) {
					issueCount++
					if owner != expOwner {
						t.Fatalf("got %s, expected %s", owner, expOwner)
					}
					if repo != expRepo {
						t.Fatalf("got %s, expected %s", repo, expRepo)
					}
					if *issue.Assignee != expAssignee {
						t.Fatalf("got %s, expected %s", *issue.Assignee, expAssignee)
					}
					if expected := fmt.Sprintf("storage: %s failed under stress", c.testName); *issue.Title != expected {
						t.Fatalf("got %s, expected %s", *issue.Title, expected)
					}
					if !issueBodyRe.MatchString(*issue.Body) {
						t.Fatalf("got:\n%s\nexpected:\n%s", *issue.Body, issueBodyRe)
					}
					if length := len(*issue.Body); length > githubIssueBodyMaximumLength {
						t.Fatalf("issue length %d exceeds (undocumented) maximum %d", length, githubIssueBodyMaximumLength)
					}
					if *issue.Milestone != expMilestone {
						t.Fatalf("expected milestone %d, but got %d", expMilestone, *issue.Milestone)
					}
					return &github.Issue{ID: github.Int64(issueID)}, nil, nil
				}

				p.searchIssues = func(_ context.Context, query string,
					opt *github.SearchOptions) (*github.IssuesSearchResult, *github.Response, error) {
					total := 0
					if foundIssue {
						total = 1
					}
					return &github.IssuesSearchResult{
						Total: &total,
						Issues: []github.Issue{
							{Number: github.Int(issueNumber)},
						},
					}, nil, nil
				}

				p.createComment = func(_ context.Context, owner string, repo string, number int,
					comment *github.IssueComment) (*github.IssueComment, *github.Response, error) {
					if owner != expOwner {
						t.Fatalf("got %s, expected %s", owner, expOwner)
					}
					if repo != expRepo {
						t.Fatalf("got %s, expected %s", repo, expRepo)
					}
					if !commentBodyRe.MatchString(*comment.Body) {
						t.Fatalf("got:\n%s\nexpected:\n%s", *comment.Body, issueBodyRe)
					}
					if length := len(*comment.Body); length > githubIssueBodyMaximumLength {
						t.Fatalf("comment length %d exceeds (undocumented) maximum %d", length, githubIssueBodyMaximumLength)
					}
					commentCount++

					return nil, nil, nil
				}

				p.listCommits = func(_ context.Context, owner string, repo string,
					opts *github.CommitsListOptions) ([]*github.RepositoryCommit, *github.Response, error) {
					if owner != expOwner {
						t.Fatalf("got %s, expected %s", owner, expOwner)
					}
					if repo != expRepo {
						t.Fatalf("got %s, expected %s", repo, expRepo)
					}
					if opts.Author == "" {
						t.Fatalf("found no author, but expected one")
					}
					assignee := expAssignee
					return []*github.RepositoryCommit{
						{
							Author: &github.User{
								Login: &assignee,
							},
						},
					}, nil, nil
				}

				p.listMilestones = func(_ context.Context, owner, repo string,
					_ *github.MilestoneListOptions) ([]*github.Milestone, *github.Response, error) {
					if owner != expOwner {
						t.Fatalf("got %s, expected %s", owner, expOwner)
					}
					if repo != expRepo {
						t.Fatalf("got %s, expected %s", repo, expRepo)
					}
					return []*github.Milestone{
						{Title: github.String("3.3"), Number: github.Int(expMilestone)},
						{Title: github.String("3.2"), Number: github.Int(1)},
					}, nil, nil
				}

				p.getLatestTag = func() (string, error) { return "v3.3.0", nil }

				p.init()

				ctx := context.Background()
				if err := p.post(
					ctx, DefaultStressFailureTitle(c.packageName, c.testName),
					c.packageName, c.testName, c.message, c.author, nil,
				); err != nil {
					t.Fatal(err)
				}

				expectedIssues := 1
				expectedComments := 0
				if foundIssue {
					expectedIssues = 0
					expectedComments = 1
				}
				if issueCount != expectedIssues {
					t.Fatalf("%d issues were posted, expected %d", issueCount, expectedIssues)
				}
				if commentCount != expectedComments {
					t.Fatalf("%d comments were posted, expected %d", commentCount, expectedComments)
				}
			})
		}
	}
}

func TestGetAssignee(t *testing.T) {
	listCommits := func(_ context.Context, owner string, repo string,
		opts *github.CommitsListOptions) ([]*github.RepositoryCommit, *github.Response, error) {
		return []*github.RepositoryCommit{
			{},
		}, nil, nil
	}
	_, _ = getAssignee(context.Background(), "", listCommits)
}

func TestInvalidAssignee(t *testing.T) {
	u, err := url.Parse("https://api.github.com/repos/cockroachdb/cockroach/issues")
	if err != nil {
		log.Fatal(err)
	}
	r := &github.ErrorResponse{
		Response: &http.Response{
			StatusCode: 422,
			Request: &http.Request{
				Method: "POST",
				URL:    u,
			},
		},
		Errors: []github.Error{{
			Resource: "Issue",
			Field:    "assignee",
			Code:     "invalid",
			Message:  "",
		}},
	}
	if !isInvalidAssignee(r) {
		t.Fatalf("expected invalid assignee")
	}
}
