package alert

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Telegram delivers alerts via the Telegram Bot API sendMessage endpoint.
// The bot token is supplied by configuration; messages are prefixed with the
// configured message_prefix and sent to the configured chat.
type Telegram struct {
	BotToken      string
	ChatID        string
	MessagePrefix string
	Timeout       time.Duration
	// Endpoint defaults to the Telegram Bot API; overridable for tests.
	Endpoint string

	// Client may be replaced for tests.
	Client *http.Client
}

// NewTelegram builds a Telegram notifier.
func NewTelegram(botToken, chatID, messagePrefix string, timeout time.Duration) *Telegram {
	return &Telegram{BotToken: botToken, ChatID: chatID, MessagePrefix: messagePrefix, Timeout: timeout}
}

// Name implements Notifier.
func (t *Telegram) Name() string { return "telegram" }

// Notify implements Notifier.
func (t *Telegram) Notify(ctx context.Context, a Alert) error {
	if t.BotToken == "" || t.ChatID == "" {
		return fmt.Errorf("telegram: bot_token and chat_id required")
	}
	text := t.render(a)
	endpoint := t.Endpoint
	if endpoint == "" {
		endpoint = "https://api.telegram.org/bot" + t.BotToken + "/sendMessage"
	}
	form := url.Values{}
	form.Set("chat_id", t.ChatID)
	form.Set("text", text)
	form.Set("parse_mode", "HTML")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := t.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram: unexpected HTTP %d", resp.StatusCode)
	}
	return nil
}

// render builds the HTML message. The text is HTML-escaped so credentials and
// node names in alerts can never inject markup.
func (t *Telegram) render(a Alert) string {
	prefix := t.MessagePrefix
	if prefix != "" {
		prefix += "\n"
	}
	severity := strings.ToUpper(string(a.Severity))
	var b bytes.Buffer
	b.WriteString(prefix)
	b.WriteString("<b>[")
	b.WriteString(escapeHTML(severity))
	b.WriteString("] ")
	b.WriteString(escapeHTML(a.Title))
	b.WriteString("</b>\n")
	b.WriteString(escapeHTML(a.Message))
	return b.String()
}

func escapeHTML(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&#39;",
	)
	return r.Replace(s)
}

func (t *Telegram) client() *http.Client {
	if t.Client != nil {
		return t.Client
	}
	timeout := t.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{Timeout: timeout}
}
