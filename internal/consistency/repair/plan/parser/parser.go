// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// WhenExpr represents a compiled predicate over diff row values (n1./n2.).
type WhenExpr struct {
	root exprNode
}

// ValueProvider returns the value for a given source ("n1" or "n2") and column.
type ValueProvider func(source, column string) (any, bool)

// Eval evaluates the predicate against the provided values.
func (w *WhenExpr) Eval(provider ValueProvider) (bool, error) {
	if w == nil || w.root == nil {
		return true, nil
	}
	val, err := w.root.eval(provider)
	if err != nil {
		return false, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("when expression did not produce a boolean result")
	}
	return b, nil
}

// CompileWhenExpression parses a restricted predicate language:
// - Identifiers: n1.col, n2.col
// - Literals: strings ('x'), numbers, booleans, NULL
// - Operators: = != < <= > >=, IN (...), IS [NOT] NULL, AND, OR, NOT, parentheses.
func CompileWhenExpression(expr string) (*WhenExpr, error) {
	lex := newLexer(expr)
	tokens, err := lex.lex()
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	root, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	return &WhenExpr{root: root}, nil
}

type tokenType int

const (
	tokEOF tokenType = iota
	tokIdent
	tokString
	tokNumber
	tokBool
	tokNull
	tokAnd
	tokOr
	tokNot
	tokIn
	tokIs
	tokLParen
	tokRParen
	tokComma
	tokEq
	tokNeq
	tokLt
	tokLte
	tokGt
	tokGte
)

type token struct {
	typ   tokenType
	lit   string
	pos   int
	value any
}

type lexer struct {
	input string
	pos   int
}

func newLexer(input string) *lexer {
	return &lexer{input: input}
}

func (l *lexer) lex() ([]token, error) {
	var toks []token
	for {
		l.skipSpace()
		if l.pos >= len(l.input) {
			toks = append(toks, token{typ: tokEOF, pos: l.pos})
			return toks, nil
		}
		ch := l.input[l.pos]
		switch ch {
		case '(':
			toks = append(toks, token{typ: tokLParen, lit: "(", pos: l.pos})
			l.pos++
		case ')':
			toks = append(toks, token{typ: tokRParen, lit: ")", pos: l.pos})
			l.pos++
		case ',':
			toks = append(toks, token{typ: tokComma, lit: ",", pos: l.pos})
			l.pos++
		case '=':
			toks = append(toks, token{typ: tokEq, lit: "=", pos: l.pos})
			l.pos++
		case '!':
			if l.peek("=") {
				toks = append(toks, token{typ: tokNeq, lit: "!=", pos: l.pos})
				l.pos += 2
			} else {
				return nil, fmt.Errorf("unexpected '!' at pos %d", l.pos)
			}
		case '<':
			if l.peek("=") {
				toks = append(toks, token{typ: tokLte, lit: "<=", pos: l.pos})
				l.pos += 2
			} else {
				toks = append(toks, token{typ: tokLt, lit: "<", pos: l.pos})
				l.pos++
			}
		case '>':
			if l.peek("=") {
				toks = append(toks, token{typ: tokGte, lit: ">=", pos: l.pos})
				l.pos += 2
			} else {
				toks = append(toks, token{typ: tokGt, lit: ">", pos: l.pos})
				l.pos++
			}
		case '\'':
			tok, err := l.scanString()
			if err != nil {
				return nil, err
			}
			toks = append(toks, tok)
		default:
			if unicode.IsDigit(rune(ch)) || ch == '-' {
				tok, err := l.scanNumber()
				if err != nil {
					return nil, err
				}
				toks = append(toks, tok)
				continue
			}
			if unicode.IsLetter(rune(ch)) || ch == '_' {
				tok, err := l.scanIdent()
				if err != nil {
					return nil, err
				}
				toks = append(toks, tok)
				continue
			}
			return nil, fmt.Errorf("unexpected character '%c' at pos %d", ch, l.pos)
		}
	}
}

func (l *lexer) peek(next string) bool {
	return strings.HasPrefix(l.input[l.pos:], next)
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *lexer) scanString() (token, error) {
	start := l.pos
	l.pos++ // skip opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				sb.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++
			return token{typ: tokString, lit: l.input[start:l.pos], pos: start, value: sb.String()}, nil
		}
		sb.WriteByte(ch)
		l.pos++
	}
	return token{}, fmt.Errorf("unterminated string starting at pos %d", start)
}

func (l *lexer) scanNumber() (token, error) {
	start := l.pos
	hasDot := false
	if l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '.' {
			if hasDot {
				break
			}
			hasDot = true
			l.pos++
			continue
		}
		if !unicode.IsDigit(rune(ch)) {
			break
		}
		l.pos++
	}
	text := l.input[start:l.pos]
	num, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return token{}, fmt.Errorf("invalid number %q at pos %d", text, start)
	}
	return token{typ: tokNumber, lit: text, pos: start, value: num}, nil
}

func (l *lexer) scanIdent() (token, error) {
	start := l.pos
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' || ch == '.' {
			l.pos++
			continue
		}
		break
	}
	text := l.input[start:l.pos]
	low := strings.ToLower(text)
	switch low {
	case "and":
		return token{typ: tokAnd, lit: text, pos: start}, nil
	case "or":
		return token{typ: tokOr, lit: text, pos: start}, nil
	case "not":
		return token{typ: tokNot, lit: text, pos: start}, nil
	case "in":
		return token{typ: tokIn, lit: text, pos: start}, nil
	case "is":
		return token{typ: tokIs, lit: text, pos: start}, nil
	case "true", "false":
		val := low == "true"
		return token{typ: tokBool, lit: text, pos: start, value: val}, nil
	case "null":
		return token{typ: tokNull, lit: text, pos: start, value: nil}, nil
	default:
		return token{typ: tokIdent, lit: text, pos: start}, nil
	}
}

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) current() token {
	if p.pos >= len(p.tokens) {
		return token{typ: tokEOF, pos: p.pos}
	}
	return p.tokens[p.pos]
}

func (p *parser) advance() {
	if p.pos < len(p.tokens) {
		p.pos++
	}
}

func (p *parser) expect(tt tokenType) (token, error) {
	tok := p.current()
	if tok.typ != tt {
		return tok, fmt.Errorf("unexpected token %q at pos %d, expected %v", tok.lit, tok.pos, tt)
	}
	p.advance()
	return tok, nil
}

func (p *parser) parseExpression() (exprNode, error) {
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.current().typ != tokEOF {
		return nil, fmt.Errorf("unexpected trailing token %q at pos %d", p.current().lit, p.current().pos)
	}
	return node, nil
}

func (p *parser) parseOr() (exprNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.current().typ == tokOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &logicalNode{op: tokOr, left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (exprNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.current().typ == tokAnd {
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &logicalNode{op: tokAnd, left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (exprNode, error) {
	if p.current().typ == tokNot {
		p.advance()
		child, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &notNode{child: child}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (exprNode, error) {
	if p.current().typ == tokLParen {
		p.advance()
		node, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return node, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (exprNode, error) {
	left, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	tok := p.current()
	switch tok.typ {
	case tokEq, tokNeq, tokLt, tokLte, tokGt, tokGte:
		p.advance()
		right, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return &comparisonNode{op: tok.typ, left: left, right: right}, nil
	case tokIn:
		p.advance()
		if _, err := p.expect(tokLParen); err != nil {
			return nil, err
		}
		var values []exprNode
		for {
			val, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			values = append(values, val)
			if p.current().typ == tokComma {
				p.advance()
				continue
			}
			if p.current().typ == tokRParen {
				p.advance()
				break
			}
			return nil, fmt.Errorf("expected ',' or ')' at pos %d", p.current().pos)
		}
		return &inNode{value: left, set: values}, nil
	case tokIs:
		p.advance()
		negate := false
		if p.current().typ == tokNot {
			negate = true
			p.advance()
		}
		if _, err := p.expect(tokNull); err != nil {
			return nil, err
		}
		return &isNullNode{value: left, negate: negate}, nil
	default:
		return nil, fmt.Errorf("expected comparison operator after value at pos %d", tok.pos)
	}
}

func (p *parser) parseValue() (exprNode, error) {
	tok := p.current()
	switch tok.typ {
	case tokIdent:
		p.advance()
		source, col, err := splitIdent(tok.lit)
		if err != nil {
			return nil, fmt.Errorf("invalid identifier %q at pos %d: %w", tok.lit, tok.pos, err)
		}
		return &identNode{source: source, column: col}, nil
	case tokString, tokNumber, tokBool, tokNull:
		p.advance()
		return &literalNode{value: tok.value}, nil
	case tokLParen:
		// allow nested parentheses via primary path
		return p.parsePrimary()
	default:
		return nil, fmt.Errorf("unexpected token %q at pos %d", tok.lit, tok.pos)
	}
}

func splitIdent(lit string) (string, string, error) {
	parts := strings.Split(lit, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("identifier must be n1.<col> or n2.<col>")
	}
	source := strings.ToLower(strings.TrimSpace(parts[0]))
	if source != "n1" && source != "n2" {
		return "", "", fmt.Errorf("identifier prefix must be n1 or n2")
	}
	col := strings.TrimSpace(parts[1])
	if col == "" {
		return "", "", fmt.Errorf("column name cannot be empty")
	}
	return source, col, nil
}

// AST nodes and evaluation
type exprNode interface {
	eval(ValueProvider) (any, error)
}

type identNode struct {
	source string
	column string
}

func (n *identNode) eval(provider ValueProvider) (any, error) {
	if provider == nil {
		return nil, fmt.Errorf("no value provider available")
	}
	val, _ := provider(n.source, n.column)
	return val, nil
}

type literalNode struct {
	value any
}

func (n *literalNode) eval(_ ValueProvider) (any, error) {
	return n.value, nil
}

type logicalNode struct {
	op    tokenType
	left  exprNode
	right exprNode
}

func (n *logicalNode) eval(provider ValueProvider) (any, error) {
	lv, err := n.left.eval(provider)
	if err != nil {
		return nil, err
	}
	lb, ok := lv.(bool)
	if !ok {
		return nil, fmt.Errorf("left side of logical op is not boolean")
	}
	if n.op == tokAnd && !lb {
		return false, nil
	}
	if n.op == tokOr && lb {
		return true, nil
	}

	rv, err := n.right.eval(provider)
	if err != nil {
		return nil, err
	}
	rb, ok := rv.(bool)
	if !ok {
		return nil, fmt.Errorf("right side of logical op is not boolean")
	}

	switch n.op {
	case tokAnd:
		return lb && rb, nil
	case tokOr:
		return lb || rb, nil
	default:
		return nil, fmt.Errorf("unknown logical operator")
	}
}

type notNode struct {
	child exprNode
}

func (n *notNode) eval(provider ValueProvider) (any, error) {
	val, err := n.child.eval(provider)
	if err != nil {
		return nil, err
	}
	b, ok := val.(bool)
	if !ok {
		return nil, fmt.Errorf("NOT operand is not boolean")
	}
	return !b, nil
}

type comparisonNode struct {
	op    tokenType
	left  exprNode
	right exprNode
}

func (n *comparisonNode) eval(provider ValueProvider) (any, error) {
	lv, err := n.left.eval(provider)
	if err != nil {
		return nil, err
	}
	rv, err := n.right.eval(provider)
	if err != nil {
		return nil, err
	}
	return compareValues(n.op, lv, rv)
}

type inNode struct {
	value exprNode
	set   []exprNode
}

func (n *inNode) eval(provider ValueProvider) (any, error) {
	val, err := n.value.eval(provider)
	if err != nil {
		return nil, err
	}
	for _, el := range n.set {
		ev, err := el.eval(provider)
		if err != nil {
			return nil, err
		}
		ok, err := compareValues(tokEq, val, ev)
		if err != nil {
			return nil, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

type isNullNode struct {
	value  exprNode
	negate bool
}

func (n *isNullNode) eval(provider ValueProvider) (any, error) {
	val, err := n.value.eval(provider)
	if err != nil {
		return nil, err
	}
	isNull := val == nil
	if n.negate {
		return !isNull, nil
	}
	return isNull, nil
}

func compareValues(op tokenType, left any, right any) (bool, error) {
	// nil handling
	if left == nil || right == nil {
		switch op {
		case tokEq:
			return left == nil && right == nil, nil
		case tokNeq:
			return !(left == nil && right == nil), nil
		default:
			return false, fmt.Errorf("cannot compare nil with operator %v", op)
		}
	}

	ln, lok := asNumber(left)
	rn, rok := asNumber(right)
	if lok && rok {
		switch op {
		case tokEq:
			return ln == rn, nil
		case tokNeq:
			return ln != rn, nil
		case tokLt:
			return ln < rn, nil
		case tokLte:
			return ln <= rn, nil
		case tokGt:
			return ln > rn, nil
		case tokGte:
			return ln >= rn, nil
		default:
			return false, fmt.Errorf("unsupported numeric operator %v", op)
		}
	}

	ls, lsok := left.(string)
	rs, rsok := right.(string)
	if lsok && rsok {
		switch op {
		case tokEq:
			return ls == rs, nil
		case tokNeq:
			return ls != rs, nil
		case tokLt:
			return ls < rs, nil
		case tokLte:
			return ls <= rs, nil
		case tokGt:
			return ls > rs, nil
		case tokGte:
			return ls >= rs, nil
		default:
			return false, fmt.Errorf("unsupported string operator %v", op)
		}
	}

	lb, lbok := left.(bool)
	rb, rbok := right.(bool)
	if lbok && rbok {
		switch op {
		case tokEq:
			return lb == rb, nil
		case tokNeq:
			return lb != rb, nil
		default:
			return false, fmt.Errorf("unsupported boolean operator %v", op)
		}
	}

	return false, fmt.Errorf("cannot compare values of different or unsupported types (%T vs %T)", left, right)
}

func asNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
