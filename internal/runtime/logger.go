package runtime

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	colorReset  = "\033[0m"
	colorCyan   = "\033[36m"
	colorBlue   = "\033[34m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
)

type BeautifulLogger struct {
	mu     sync.Mutex
	logger *log.Logger
	debug  bool
	color  bool
}

var defaultLogger = NewBeautifulLogger(true)

func NewBeautifulLogger(debug bool) *BeautifulLogger {
	return &BeautifulLogger{
		logger: log.New(os.Stdout, "", 0),
		debug:  debug,
		color:  true,
	}
}

func SetDefaultLogger(logger *BeautifulLogger) {
	if logger == nil {
		return
	}
	defaultLogger = logger
}

func DefaultLogger() *BeautifulLogger {
	return defaultLogger
}

func (l *BeautifulLogger) Info(component, format string, args ...any) {
	l.write("INFO", colorBlue, component, format, args...)
}

func (l *BeautifulLogger) Debug(component, format string, args ...any) {
	if !l.debug {
		return
	}
	l.write("DEBUG", colorCyan, component, format, args...)
}

func (l *BeautifulLogger) Error(component, format string, args ...any) {
	l.write("ERROR", colorRed, component, format, args...)
}

func (l *BeautifulLogger) Result(streamID string, value any) {
	l.write("RESULT", colorGreen, "sink", "%s <= %s", streamID, DescribeValue(value))
}

func (l *BeautifulLogger) write(level, color, component, format string, args ...any) {
	if l == nil {
		return
	}

	message := fmt.Sprintf(format, args...)
	now := time.Now().Format("15:04:05.000")

	levelText := pad(level, 6)
	componentText := pad(component, 10)

	if l.color {
		levelText = color + levelText + colorReset
	}

	line := fmt.Sprintf("%s | %s | %s | %s", now, levelText, componentText, message)

	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println(line)
}

func DescribeValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "<nil>"
	case fmt.Stringer:
		return typed.String()
	case bool:
		return fmt.Sprintf("%t", typed)
	case string:
		return fmt.Sprintf("%q", typed)
	default:
		return fmt.Sprintf("%+v", typed)
	}
}

func pad(value string, width int) string {
	if len(value) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-len(value))
}
