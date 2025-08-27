package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RedisType Redis数据类型
type RedisType byte

const (
	TypeSimpleString RedisType = '+'
	TypeError       RedisType = '-'
	TypeInteger     RedisType = ':'
	TypeBulkString  RedisType = '$'
	TypeArray       RedisType = '*'
)

// RedisValue Redis值
type RedisValue struct {
	Type     RedisType
	Str      string
	Int      int64
	Bulk     []byte
	Array    []*RedisValue
	IsNull   bool
	RawBytes []byte // 原始字节数据
}

// Parser Redis协议解析器
type Parser struct {
	reader *bufio.Reader
	buf    []byte
}

// NewParser 创建解析器
func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(r),
		buf:    make([]byte, 0, 1024),
	}
}

// Parse 解析Redis协议
func (p *Parser) Parse() (*RedisValue, error) {
	// 读取类型标识符
	typeByte, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	
	switch RedisType(typeByte) {
	case TypeSimpleString:
		return p.parseSimpleString()
	case TypeError:
		return p.parseError()
	case TypeInteger:
		return p.parseInteger()
	case TypeBulkString:
		return p.parseBulkString()
	case TypeArray:
		return p.parseArray()
	default:
		return nil, fmt.Errorf("unknown redis type: %c", typeByte)
	}
}

// parseSimpleString 解析简单字符串
func (p *Parser) parseSimpleString() (*RedisValue, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	return &RedisValue{
		Type:     TypeSimpleString,
		Str:      string(line),
		RawBytes: append([]byte{'+'}, append(line, '\r', '\n')...),
	}, nil
}

// parseError 解析错误
func (p *Parser) parseError() (*RedisValue, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	return &RedisValue{
		Type:     TypeError,
		Str:      string(line),
		RawBytes: append([]byte{'-'}, append(line, '\r', '\n')...),
	}, nil
}

// parseInteger 解析整数
func (p *Parser) parseInteger() (*RedisValue, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	num, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid integer: %s", line)
	}
	
	return &RedisValue{
		Type:     TypeInteger,
		Int:      num,
		RawBytes: append([]byte{':'}, append(line, '\r', '\n')...),
	}, nil
}

// parseBulkString 解析批量字符串
func (p *Parser) parseBulkString() (*RedisValue, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	length, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, fmt.Errorf("invalid bulk string length: %s", line)
	}
	
	// 处理NULL bulk string
	if length == -1 {
		return &RedisValue{
			Type:     TypeBulkString,
			IsNull:   true,
			RawBytes: []byte("$-1\r\n"),
		}, nil
	}
	
	if length < 0 {
		return nil, fmt.Errorf("invalid bulk string length: %d", length)
	}
	
	// 读取指定长度的数据
	data := make([]byte, length)
	if _, err := io.ReadFull(p.reader, data); err != nil {
		return nil, err
	}
	
	// 读取结尾的\r\n
	if _, err := p.readLine(); err != nil {
		return nil, err
	}
	
	// 构建原始字节数据
	rawBytes := make([]byte, 0, len(line)+len(data)+6)
	rawBytes = append(rawBytes, '$')
	rawBytes = append(rawBytes, line...)
	rawBytes = append(rawBytes, '\r', '\n')
	rawBytes = append(rawBytes, data...)
	rawBytes = append(rawBytes, '\r', '\n')
	
	return &RedisValue{
		Type:     TypeBulkString,
		Bulk:     data,
		RawBytes: rawBytes,
	}, nil
}

// parseArray 解析数组
func (p *Parser) parseArray() (*RedisValue, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}
	
	count, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", line)
	}
	
	// 处理NULL array
	if count == -1 {
		return &RedisValue{
			Type:     TypeArray,
			IsNull:   true,
			RawBytes: []byte("*-1\r\n"),
		}, nil
	}
	
	if count < 0 {
		return nil, fmt.Errorf("invalid array length: %d", count)
	}
	
	// 解析数组元素
	array := make([]*RedisValue, count)
	rawBytes := make([]byte, 0, 1024)
	rawBytes = append(rawBytes, '*')
	rawBytes = append(rawBytes, line...)
	rawBytes = append(rawBytes, '\r', '\n')
	
	for i := 0; i < count; i++ {
		value, err := p.Parse()
		if err != nil {
			return nil, err
		}
		array[i] = value
		rawBytes = append(rawBytes, value.RawBytes...)
	}
	
	return &RedisValue{
		Type:     TypeArray,
		Array:    array,
		RawBytes: rawBytes,
	}, nil
}

// readLine 读取一行数据（不包含\r\n）
func (p *Parser) readLine() ([]byte, error) {
	p.buf = p.buf[:0]
	
	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		
		if b == '\r' {
			// 检查下一个字节是否为\n
			next, err := p.reader.ReadByte()
			if err != nil {
				return nil, err
			}
			if next != '\n' {
				return nil, fmt.Errorf("expected \\n after \\r, got %c", next)
			}
			break
		}
		
		p.buf = append(p.buf, b)
	}
	
	// 返回副本
	result := make([]byte, len(p.buf))
	copy(result, p.buf)
	return result, nil
}

// String 返回RedisValue的字符串表示
func (v *RedisValue) String() string {
	if v.IsNull {
		return "(null)"
	}
	
	switch v.Type {
	case TypeSimpleString:
		return v.Str
	case TypeError:
		return fmt.Sprintf("(error) %s", v.Str)
	case TypeInteger:
		return fmt.Sprintf("(integer) %d", v.Int)
	case TypeBulkString:
		return fmt.Sprintf("\"%s\"", string(v.Bulk))
	case TypeArray:
		if len(v.Array) == 0 {
			return "(empty list or set)"
		}
		var parts []string
		for i, item := range v.Array {
			parts = append(parts, fmt.Sprintf("%d) %s", i+1, item.String()))
		}
		return strings.Join(parts, "\n")
	default:
		return "(unknown)"
	}
}

// IsCommand 检查是否为Redis命令
func (v *RedisValue) IsCommand() bool {
	return v.Type == TypeArray && len(v.Array) > 0 && !v.IsNull
}

// GetCommand 获取命令名称
func (v *RedisValue) GetCommand() string {
	if !v.IsCommand() {
		return ""
	}
	
	first := v.Array[0]
	if first.Type == TypeBulkString && !first.IsNull {
		return strings.ToUpper(string(first.Bulk))
	}
	
	return ""
}

// GetArgs 获取命令参数
func (v *RedisValue) GetArgs() [][]byte {
	if !v.IsCommand() || len(v.Array) <= 1 {
		return nil
	}
	
	args := make([][]byte, 0, len(v.Array)-1)
	for i := 1; i < len(v.Array); i++ {
		arg := v.Array[i]
		if arg.Type == TypeBulkString && !arg.IsNull {
			args = append(args, arg.Bulk)
		}
	}
	
	return args
}

// GetKey 获取命令的key（第一个参数）
func (v *RedisValue) GetKey() []byte {
	args := v.GetArgs()
	if len(args) > 0 {
		return args[0]
	}
	return nil
}

// GetKeys 获取命令涉及的所有key
func (v *RedisValue) GetKeys() [][]byte {
	cmd := v.GetCommand()
	args := v.GetArgs()
	
	switch cmd {
	case "GET", "SET", "DEL", "EXISTS", "EXPIRE", "TTL", "TYPE":
		// 单key命令
		if len(args) > 0 {
			return [][]byte{args[0]}
		}
	case "MGET", "MSET", "MSETNX":
		// 多key命令
		if cmd == "MSET" || cmd == "MSETNX" {
			// MSET key1 value1 key2 value2 ...
			keys := make([][]byte, 0, len(args)/2)
			for i := 0; i < len(args); i += 2 {
				keys = append(keys, args[i])
			}
			return keys
		} else {
			// MGET key1 key2 key3 ...
			return args
		}
	case "RENAME", "RENAMENX", "MOVE":
		// 双key命令
		if len(args) >= 2 {
			return [][]byte{args[0], args[1]}
		}
	default:
		// 默认返回第一个参数作为key
		if len(args) > 0 {
			return [][]byte{args[0]}
		}
	}
	
	return nil
}

// Serialize 序列化为Redis协议格式
func (v *RedisValue) Serialize() []byte {
	if len(v.RawBytes) > 0 {
		return v.RawBytes
	}
	
	var buf bytes.Buffer
	
	switch v.Type {
	case TypeSimpleString:
		buf.WriteByte('+')
		buf.WriteString(v.Str)
		buf.WriteString("\r\n")
	case TypeError:
		buf.WriteByte('-')
		buf.WriteString(v.Str)
		buf.WriteString("\r\n")
	case TypeInteger:
		buf.WriteByte(':')
		buf.WriteString(strconv.FormatInt(v.Int, 10))
		buf.WriteString("\r\n")
	case TypeBulkString:
		if v.IsNull {
			buf.WriteString("$-1\r\n")
		} else {
			buf.WriteByte('$')
			buf.WriteString(strconv.Itoa(len(v.Bulk)))
			buf.WriteString("\r\n")
			buf.Write(v.Bulk)
			buf.WriteString("\r\n")
		}
	case TypeArray:
		if v.IsNull {
			buf.WriteString("*-1\r\n")
		} else {
			buf.WriteByte('*')
			buf.WriteString(strconv.Itoa(len(v.Array)))
			buf.WriteString("\r\n")
			for _, item := range v.Array {
				buf.Write(item.Serialize())
			}
		}
	}
	
	return buf.Bytes()
}