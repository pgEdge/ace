// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2026, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// WhenExpr represents a compiled predicate over diff row values (n1./n2.).
type WhenExpr struct {
	root exprNode
}

// Value is a value in a when expression: the text of a column value or of a
// literal, or NULL.
type Value struct {
	Text string
	Null bool
}

// ValueProvider returns the value of a column of a source ("n1" or "n2"). A
// column that the row does not have is NULL.
type ValueProvider func(source, column string) Value

// Comparison is one comparison of two values that are not NULL. Op is one of
// =, <>, <, <=, > and >=. A side that is a column names it in LeftColumn or
// RightColumn; a literal has an empty name. At least one side is a column,
// and the server compares the texts with the type of the column.
type Comparison struct {
	Op                      string
	Left, Right             string
	LeftColumn, RightColumn string
}

// Comparator returns the result of a comparison.
type Comparator func(c Comparison) (bool, error)

// Eval evaluates the predicate against the provided values. A comparison
// with a NULL side is decided here; every other comparison goes to cmp.
func (w *WhenExpr) Eval(provider ValueProvider, cmp Comparator) (bool, error) {
	if w == nil || w.root == nil {
		return true, nil
	}
	val, err := w.root.eval(provider, cmp)
	if err != nil {
		return false, err
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("when expression did not produce a boolean result")
	}
	return b, nil
}

// Comparisons returns every comparison that Eval can give to its comparator
// for these values, so that a caller can compute them all at once before
// Eval. It does not skip the parts that AND and OR would not evaluate.
func (w *WhenExpr) Comparisons(provider ValueProvider) []Comparison {
	if w == nil || w.root == nil {
		return nil
	}
	var out []Comparison
	w.root.comparisons(provider, &out)
	return out
}

// CompileWhenExpression parses a restricted predicate language:
// - Identifiers: n1.col, n2.col
// - Literals: strings ('x'), numbers, booleans, NULL
// - Operators: = != < <= > >=, IN (...), IS [NOT] NULL, AND, OR, NOT, parentheses.
//
// A literal keeps its text: the number 1.50 is the text 1.50. A comparison
// needs a column on at least one side, whose type the server uses to read
// both texts.
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
			return token{typ: tokString, lit: l.input[start:l.pos], pos: start, value: Value{Text: sb.String()}}, nil
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
	return token{typ: tokNumber, lit: text, pos: start, value: Value{Text: text}}, nil
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
		return token{typ: tokBool, lit: text, pos: start, value: Value{Text: low}}, nil
	case "null":
		return token{typ: tokNull, lit: text, pos: start, value: Value{Null: true}}, nil
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
		if err := needColumn(left, right, tok.pos); err != nil {
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
			if err := needColumn(left, val, tok.pos); err != nil {
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
		return &literalNode{lit: tok.value.(Value)}, nil
	case tokLParen:
		// allow nested parentheses via primary path
		return p.parsePrimary()
	default:
		return nil, fmt.Errorf("unexpected token %q at pos %d", tok.lit, tok.pos)
	}
}

// needColumn returns an error if neither side of a comparison is a column:
// two literals have no column type to compare them with.
func needColumn(left, right exprNode, pos int) error {
	_, l := left.(*identNode)
	_, r := right.(*identNode)
	if !l && !r {
		return fmt.Errorf("comparison at pos %d needs a column n1.<col> or n2.<col> on one side", pos)
	}
	return nil
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
	eval(ValueProvider, Comparator) (any, error)
	comparisons(ValueProvider, *[]Comparison)
}

// valueNode is a node that gives a value: a column or a literal.
type valueNode interface {
	exprNode
	value(ValueProvider) (Value, string)
}

type identNode struct {
	source string
	column string
}

func (n *identNode) eval(provider ValueProvider, _ Comparator) (any, error) {
	return nil, fmt.Errorf("column %s.%s is not a boolean", n.source, n.column)
}

func (n *identNode) comparisons(ValueProvider, *[]Comparison) {}

// value returns the value of the column and its name.
func (n *identNode) value(provider ValueProvider) (Value, string) {
	if provider == nil {
		return Value{Null: true}, n.column
	}
	return provider(n.source, n.column), n.column
}

type literalNode struct {
	lit Value
}

func (n *literalNode) eval(ValueProvider, Comparator) (any, error) {
	return nil, fmt.Errorf("a literal is not a boolean")
}

func (n *literalNode) comparisons(ValueProvider, *[]Comparison) {}

func (n *literalNode) value(ValueProvider) (Value, string) { return n.lit, "" }

type logicalNode struct {
	op    tokenType
	left  exprNode
	right exprNode
}

func (n *logicalNode) eval(provider ValueProvider, cmp Comparator) (any, error) {
	lv, err := n.left.eval(provider, cmp)
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

	rv, err := n.right.eval(provider, cmp)
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

func (n *logicalNode) comparisons(provider ValueProvider, out *[]Comparison) {
	n.left.comparisons(provider, out)
	n.right.comparisons(provider, out)
}

type notNode struct {
	child exprNode
}

func (n *notNode) eval(provider ValueProvider, cmp Comparator) (any, error) {
	val, err := n.child.eval(provider, cmp)
	if err != nil {
		return nil, err
	}
	b, ok := val.(bool)
	if !ok {
		return nil, fmt.Errorf("NOT operand is not boolean")
	}
	return !b, nil
}

func (n *notNode) comparisons(provider ValueProvider, out *[]Comparison) {
	n.child.comparisons(provider, out)
}

type comparisonNode struct {
	op    tokenType
	left  exprNode
	right exprNode
}

func (n *comparisonNode) eval(provider ValueProvider, cmp Comparator) (any, error) {
	return compare(n.op, n.left, n.right, provider, cmp)
}

func (n *comparisonNode) comparisons(provider ValueProvider, out *[]Comparison) {
	addComparison(n.op, n.left, n.right, provider, out)
}

type inNode struct {
	value exprNode
	set   []exprNode
}

func (n *inNode) eval(provider ValueProvider, cmp Comparator) (any, error) {
	for _, el := range n.set {
		ok, err := compare(tokEq, n.value, el, provider, cmp)
		if err != nil {
			return nil, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (n *inNode) comparisons(provider ValueProvider, out *[]Comparison) {
	for _, el := range n.set {
		addComparison(tokEq, n.value, el, provider, out)
	}
}

type isNullNode struct {
	value  exprNode
	negate bool
}

func (n *isNullNode) eval(provider ValueProvider, _ Comparator) (any, error) {
	vn, ok := n.value.(valueNode)
	if !ok {
		return nil, fmt.Errorf("IS NULL needs a column or a literal")
	}
	v, _ := vn.value(provider)
	if n.negate {
		return !v.Null, nil
	}
	return v.Null, nil
}

func (n *isNullNode) comparisons(ValueProvider, *[]Comparison) {}

// sqlOps are the operators of the comparisons, as SQL writes them.
var sqlOps = map[tokenType]string{
	tokEq: "=", tokNeq: "<>", tokLt: "<", tokLte: "<=", tokGt: ">", tokGte: ">=",
}

// comparisonOf returns the comparison of two value nodes, and false if a side
// is NULL: then the comparison is decided without the server.
func comparisonOf(op tokenType, left, right exprNode, provider ValueProvider) (Comparison, Value, Value, error) {
	ln, lok := left.(valueNode)
	rn, rok := right.(valueNode)
	if !lok || !rok {
		return Comparison{}, Value{}, Value{}, fmt.Errorf("a comparison needs a column or a literal on each side")
	}
	lv, lcol := ln.value(provider)
	rv, rcol := rn.value(provider)
	sqlOp, ok := sqlOps[op]
	if !ok {
		return Comparison{}, Value{}, Value{}, fmt.Errorf("unknown comparison operator %v", op)
	}
	return Comparison{Op: sqlOp, Left: lv.Text, Right: rv.Text, LeftColumn: lcol, RightColumn: rcol}, lv, rv, nil
}

func addComparison(op tokenType, left, right exprNode, provider ValueProvider, out *[]Comparison) {
	c, lv, rv, err := comparisonOf(op, left, right, provider)
	if err != nil || lv.Null || rv.Null {
		return
	}
	*out = append(*out, c)
}

// compare decides a comparison. With a NULL side, = is true only if both
// sides are NULL and != is its negation; the other operators give an error.
func compare(op tokenType, left, right exprNode, provider ValueProvider, cmp Comparator) (bool, error) {
	c, lv, rv, err := comparisonOf(op, left, right, provider)
	if err != nil {
		return false, err
	}
	if lv.Null || rv.Null {
		switch op {
		case tokEq:
			return lv.Null && rv.Null, nil
		case tokNeq:
			return !(lv.Null && rv.Null), nil
		default:
			return false, fmt.Errorf("cannot compare NULL with operator %s", c.Op)
		}
	}
	if cmp == nil {
		return false, fmt.Errorf("no comparator for %s %s %s", c.Left, c.Op, c.Right)
	}
	return cmp(c)
}
