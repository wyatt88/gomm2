// Package filter provides topic, group, and config property filters.
package filter

import (
	"regexp"
	"strings"
)

// TopicFilter decides whether a topic should be replicated.
type TopicFilter struct {
	whitelist []*regexp.Regexp
	blacklist []*regexp.Regexp
}

// NewTopicFilter creates a TopicFilter from whitelist/blacklist regex patterns.
// An empty whitelist means "match all". Blacklist takes precedence.
func NewTopicFilter(whitelist, blacklist []string) (*TopicFilter, error) {
	wl, err := compilePatterns(whitelist)
	if err != nil {
		return nil, err
	}
	bl, err := compilePatterns(blacklist)
	if err != nil {
		return nil, err
	}
	return &TopicFilter{whitelist: wl, blacklist: bl}, nil
}

// ShouldReplicate returns true if the topic should be replicated.
func (tf *TopicFilter) ShouldReplicate(topic string) bool {
	// Skip internal topics
	if strings.HasPrefix(topic, "__") || strings.HasPrefix(topic, ".") {
		return false
	}
	for _, re := range tf.blacklist {
		if re.MatchString(topic) {
			return false
		}
	}
	if len(tf.whitelist) == 0 {
		return true
	}
	for _, re := range tf.whitelist {
		if re.MatchString(topic) {
			return true
		}
	}
	return false
}

// GroupFilter decides whether a consumer group should have its offsets synced.
type GroupFilter struct {
	whitelist []*regexp.Regexp
	blacklist []*regexp.Regexp
}

// NewGroupFilter creates a GroupFilter from whitelist/blacklist regex patterns.
func NewGroupFilter(whitelist, blacklist []string) (*GroupFilter, error) {
	wl, err := compilePatterns(whitelist)
	if err != nil {
		return nil, err
	}
	bl, err := compilePatterns(blacklist)
	if err != nil {
		return nil, err
	}
	return &GroupFilter{whitelist: wl, blacklist: bl}, nil
}

// ShouldReplicate returns true if the consumer group offsets should be synced.
func (gf *GroupFilter) ShouldReplicate(group string) bool {
	for _, re := range gf.blacklist {
		if re.MatchString(group) {
			return false
		}
	}
	if len(gf.whitelist) == 0 {
		return true
	}
	for _, re := range gf.whitelist {
		if re.MatchString(group) {
			return true
		}
	}
	return false
}

// ConfigPropertyFilter decides which topic config properties to replicate.
type ConfigPropertyFilter struct {
	whitelist []*regexp.Regexp
	blacklist []*regexp.Regexp
}

// NewConfigPropertyFilter creates a filter for topic config properties.
func NewConfigPropertyFilter(whitelist, blacklist []string) (*ConfigPropertyFilter, error) {
	wl, err := compilePatterns(whitelist)
	if err != nil {
		return nil, err
	}
	bl, err := compilePatterns(blacklist)
	if err != nil {
		return nil, err
	}
	return &ConfigPropertyFilter{whitelist: wl, blacklist: bl}, nil
}

// ShouldReplicate returns true if the config property should be replicated.
func (cf *ConfigPropertyFilter) ShouldReplicate(property string) bool {
	for _, re := range cf.blacklist {
		if re.MatchString(property) {
			return false
		}
	}
	if len(cf.whitelist) == 0 {
		return true
	}
	for _, re := range cf.whitelist {
		if re.MatchString(property) {
			return true
		}
	}
	return false
}

func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}
