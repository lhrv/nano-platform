import json
from pygments import highlight
from pygments.formatters.terminal256 import TerminalTrueColorFormatter
from pygments.lexers.web import JsonLexer
from pygments.lexers.data import YamlLexer
from pygments.styles import STYLE_MAP
import yaml
from yaml import CLoader as Loader, CDumper as Dumper

STYLE_NAME = "monokai"

def colorize_json_string(json_string, force_indent=False, yml=False):
    """Colorize json string and indent (optionnal)"""

    if force_indent:
        json_string = json.dumps(json.loads(json_string), indent=4)
    if yml:
        yaml_string = yaml.dump(yaml.load(json_string, Loader=Loader), Dumper=Dumper)

        return highlight(yaml_string, YamlLexer(), TerminalTrueColorFormatter(style=STYLE_NAME))

    colored = highlight(
        json_string,
        lexer=JsonLexer(),
        formatter=TerminalTrueColorFormatter(style=STYLE_NAME)
    )
    if yml:
        colored = highlight(
            yaml_string,
            lexer=YamlLexer(),
            formatter=TerminalTrueColorFormatter(style=STYLE_NAME)
        )
    return colored