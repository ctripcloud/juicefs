package cli

// CommandCategories interface allows for category manipulation
type CommandCategories interface {
	// AddCommand adds a command to a category, creating a new category if necessary.
	AddCommand(category string, command *Command)
	// Categories returns a slice of categories sorted by name
	Categories() []CommandCategory
}

type commandCategories []*commandCategory

func newCommandCategories() CommandCategories {
	ret := commandCategories([]*commandCategory{})
	return &ret
}

func (c *commandCategories) Less(i, j int) bool {
	return lexicographicLess((*c)[i].Name(), (*c)[j].Name())
}

func (c *commandCategories) Len() int {
	return len(*c)
}

func (c *commandCategories) Swap(i, j int) {
	(*c)[i], (*c)[j] = (*c)[j], (*c)[i]
}

func (c *commandCategories) AddCommand(category string, command *Command) {
	for _, commandCategory := range []*commandCategory(*c) {
		if commandCategory.name == category {
			commandCategory.commands = append(commandCategory.commands, command)
			return
		}
	}
	newVal := append(*c,
		&commandCategory{name: category, commands: []*Command{command}})
	*c = newVal
}

func (c *commandCategories) Categories() []CommandCategory {
	ret := make([]CommandCategory, len(*c))
	for i, cat := range *c {
		ret[i] = cat
	}
	return ret
}

// CommandCategory is a category containing commands.
type CommandCategory interface {
	// Name returns the category name string
	Name() string
	// VisibleCommands returns a slice of the Commands with Hidden=false
	VisibleCommands() []*Command
}

type commandCategory struct {
	name     string
	commands []*Command
}

func (c *commandCategory) Name() string {
	return c.name
}

func (c *commandCategory) VisibleCommands() []*Command {
	if c.commands == nil {
		c.commands = []*Command{}
	}

	var ret []*Command
	for _, command := range c.commands {
		if !command.Hidden {
			ret = append(ret, command)
		}
	}
	return ret
}

// FlagCategories interface allows for category manipulation
type FlagCategories interface {
	// AddFlags adds a flag to a category, creating a new category if necessary.
	AddFlag(category string, fl Flag)
	// VisibleCategories returns a slice of visible flag categories sorted by name
	VisibleCategories() []VisibleFlagCategory
}

type defaultFlagCategories struct {
	names []string
	m     map[string]*defaultVisibleFlagCategory
}

func newFlagCategories() *defaultFlagCategories {
	return &defaultFlagCategories{
		names: []string{""},
		m: map[string]*defaultVisibleFlagCategory{
			"": {},
		},
	}
}

func newFlagCategoriesFromFlags(fs []Flag) FlagCategories {
	fc := newFlagCategories()

	for _, fl := range fs {
		if cf, ok := fl.(CategorizableFlag); ok {
			if fl.(VisibleFlag).IsVisible() {
				fc.AddFlag(cf.GetCategory(), cf)
			}
		}
	}

	return fc
}

func (f *defaultFlagCategories) AddFlag(category string, fl Flag) {
	if _, ok := f.m[category]; !ok {
		f.m[category] = &defaultVisibleFlagCategory{name: category}
		f.names = append(f.names, category)
	}

	f.m[category].flags = append(f.m[category].flags, fl)
}

func (f *defaultFlagCategories) VisibleCategories() []VisibleFlagCategory {
	if len(f.names) == 1 {
		return nil // no category
	}

	ret := make([]VisibleFlagCategory, len(f.names))
	for i, name := range f.names {
		ret[i] = f.m[name]
	}

	return ret
}

// VisibleFlagCategory is a category containing flags.
type VisibleFlagCategory interface {
	// Name returns the category name string
	Name() string
	// Flags returns a slice of VisibleFlag sorted by name
	Flags() []VisibleFlag
}

type defaultVisibleFlagCategory struct {
	name  string
	flags []Flag
}

func (fc *defaultVisibleFlagCategory) Name() string {
	return fc.name
}

func (fc *defaultVisibleFlagCategory) Flags() []VisibleFlag {
	ret := make([]VisibleFlag, len(fc.flags))
	for i, fl := range fc.flags {
		ret[i] = fl.(VisibleFlag)
	}

	return ret
}
