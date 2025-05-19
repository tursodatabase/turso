# Config

Config folder should be located at `$HOME/.config/limbo`. The config file inside should be named `limbo.toml`. Optionally you can have a `themes` folder whithin to store `.tmTheme` files to be discovered by the CLI on startup. 

Describes the Limbo Config file for the CLI\

**Note**: Colors can be inputted as
- Rrggbb string -> `"#010101"`
- Rgb string -> `"#A3F"`
- 256 Ansi Color -> `"100"`
- Predefined Color Names:
  - `"black"`
  - `"red"`
  - `"green"`
  - `"yellow"`
  - `"blue"`
  - `"purple"`
  - `"cyan"`
  - `"magenta"`
  - `"white"`
  - `"grey"`
  - `"bright-black"`
  - `"bright-red"`
  - `"bright-green"`
  - `"bright-yellow"`
  - `"bright-blue"`
  - `"bright-cyan"`
  - `"bright-magenta"`
  - `"bright-white"`
  - `"dark-red"`
  - `"dark-green"`
  - `"dark-yellow"`
  - `"dark-blue"`
  - `"dark-magenta"`
  - `"dark-cyan"`
  - `"dark-grey"`

## `table`

### `column_colors`
**Type**: `List[Color]`\
*Example*: `["cyan"]`

### `header_color`
**Type**: `Color`\
*Example*: `"red"`

## `highlight`

### `enable`
**Type**: `bool`\
*Example*: `true`

### `theme`
**Type**: `String`\
*Example*: `"base16-ocean.dark"`

Preloaded themes:
- `base16-ocean.dark`
- `base16-eighties.dark`
- `base16-mocha.dark`
- `base16-ocean.light`

You can reference a custom theme in your `themes` folder directly by name from the config file.

*Example*: 

Folder structure

```
limbo
├── limbo.toml
└── themes
    └── Amy.tmTheme
```

`limbo.toml`

```toml
[highlight]
theme = "Amy"
```

### `prompt`
**Type**: `Color`\
*Example*: `"green"`

### `hint`
**Type**: `Color`\
*Example*: `"grey"`

### `candidate`
**Type**: `Color`\
*Example*: `"yellow"`

## Example `limbo.toml`

```toml
[table]
column_colors = ["cyan", "black", "#010101"]
header_color = "red"

[highlight]
enable = true
prompt = "bright-blue"
theme = "base16-ocean.light"
hint = "123"
candidate = "dark-yellow"
```

