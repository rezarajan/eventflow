package registry

// Defaults constructs the core generator registry without domain-specific generators.
func Defaults() (*Registry, error) {
	return New(), nil
}
