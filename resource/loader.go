package resource

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

var envelopeFields = map[string]bool{
	"apiVersion": true,
	"kind":       true,
	"metadata":   true,
	"spec":       true,
}

func LoadFiles(paths ...string) ([]Document, error) {
	var docs []Document
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		loaded, err := loadBytes(body, path)
		if err != nil {
			return nil, err
		}
		docs = append(docs, loaded...)
	}
	return docs, nil
}

func loadBytes(body []byte, source string) ([]Document, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	var docs []Document
	for index := 0; ; index++ {
		var node yaml.Node
		if err := decoder.Decode(&node); err != nil {
			if err == io.EOF {
				return docs, nil
			}
			return nil, typed(ErrInvalidDocument, source, fmt.Errorf("parse YAML: %w", err))
		}
		if isEmptyDocument(&node) {
			continue
		}
		doc, err := decodeDocument(&node, source, index)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
}

func decodeDocument(root *yaml.Node, source string, index int) (Document, error) {
	node := root
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		node = node.Content[0]
	}
	if node.Kind != yaml.MappingNode {
		return Document{}, typed(ErrInvalidDocument, source, fmt.Errorf("document %d must be a mapping", index))
	}
	var doc Document
	doc.Source = source
	doc.Index = index
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		value := node.Content[i+1]
		if !envelopeFields[key] {
			return Document{}, typed(ErrInvalidDocument, source, fmt.Errorf("document %d unknown field %q", index, key))
		}
		switch key {
		case "apiVersion":
			doc.APIVersion = strings.TrimSpace(value.Value)
		case "kind":
			doc.Kind = strings.TrimSpace(value.Value)
		case "metadata":
			if err := value.Decode(&doc.Metadata); err != nil {
				return Document{}, typed(ErrInvalidDocument, source, fmt.Errorf("document %d metadata: %w", index, err))
			}
		case "spec":
			doc.Spec = value
		}
	}
	if doc.APIVersion != APIVersion {
		return Document{}, typed(ErrInvalidDocument, source, fmt.Errorf("document %d apiVersion must be %s", index, APIVersion))
	}
	if doc.Kind == "" {
		return Document{}, typed(ErrInvalidDocument, source, fmt.Errorf("document %d kind is required", index))
	}
	if strings.TrimSpace(doc.Metadata.Name) == "" {
		return Document{}, typed(ErrInvalidDocument, source, fmt.Errorf("document %d metadata.name is required", index))
	}
	if doc.Spec == nil {
		empty := &yaml.Node{Kind: yaml.MappingNode}
		doc.Spec = empty
	}
	return doc, nil
}

func isEmptyDocument(node *yaml.Node) bool {
	if node == nil {
		return true
	}
	if node.Kind == yaml.DocumentNode && len(node.Content) == 0 {
		return true
	}
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		return isEmptyDocument(node.Content[0])
	}
	return node.Kind == 0
}
