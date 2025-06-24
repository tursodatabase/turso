from typing import Any, Optional, Union

from rich.console import Console, JustifyMethod
from rich.style import Style
from rich.theme import Theme

custom_theme = Theme(
    {
        "info": "bold blue",
        "error": "bold red",
        "debug": "bold blue",
        "test": "bold green",
    }
)
console = Console(theme=custom_theme, force_terminal=True)


def info(
    *objects: Any,
    sep: str = " ",
    end: str = "\n",
    style: Optional[Union[str, Style]] = None,
    justify: Optional[JustifyMethod] = None,
    emoji: Optional[bool] = None,
    markup: Optional[bool] = None,
    highlight: Optional[bool] = None,
    log_locals: bool = False,
    _stack_offset: int = 1,
):
    console.log(
        "[info]INFO[/info]",
        *objects,
        sep=sep,
        end=end,
        style=style,
        justify=justify,
        emoji=emoji,
        markup=markup,
        highlight=highlight,
        log_locals=log_locals,
        _stack_offset=_stack_offset + 1,
    )


def error(
    *objects: Any,
    sep: str = " ",
    end: str = "\n",
    style: Optional[Union[str, Style]] = None,
    justify: Optional[JustifyMethod] = None,
    emoji: Optional[bool] = None,
    markup: Optional[bool] = None,
    highlight: Optional[bool] = None,
    log_locals: bool = False,
    _stack_offset: int = 1,
):
    console.log(
        "[error]ERROR[/error]",
        *objects,
        sep=sep,
        end=end,
        style=style,
        justify=justify,
        emoji=emoji,
        markup=markup,
        highlight=highlight,
        log_locals=log_locals,
        _stack_offset=_stack_offset + 1,
    )


def debug(
    *objects: Any,
    sep: str = " ",
    end: str = "\n",
    style: Optional[Union[str, Style]] = None,
    justify: Optional[JustifyMethod] = None,
    emoji: Optional[bool] = None,
    markup: Optional[bool] = None,
    highlight: Optional[bool] = None,
    log_locals: bool = False,
    _stack_offset: int = 1,
):
    console.log(
        "[debug]DEBUG[/debug]",
        *objects,
        sep=sep,
        end=end,
        style=style,
        justify=justify,
        emoji=emoji,
        markup=markup,
        highlight=highlight,
        log_locals=log_locals,
        _stack_offset=_stack_offset + 1,
    )


def test(
    *objects: Any,
    sep: str = " ",
    end: str = "\n",
    style: Optional[Union[str, Style]] = None,
    justify: Optional[JustifyMethod] = None,
    emoji: Optional[bool] = None,
    markup: Optional[bool] = None,
    highlight: Optional[bool] = None,
    log_locals: bool = False,
    _stack_offset: int = 1,
):
    console.log(
        "[test]TEST[/test]",
        *objects,
        sep=sep,
        end=end,
        style=style,
        justify=justify,
        emoji=emoji,
        markup=markup,
        highlight=highlight,
        log_locals=log_locals,
        _stack_offset=_stack_offset + 1,
    )
