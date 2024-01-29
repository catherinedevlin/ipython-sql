from traitlets import TraitError, TraitType
from sql import display
import warnings

VALUE_WARNING = (
    'Please use a valid option: "warn", "enabled", or "disabled". \n'
    "For more information, "
    "see the docs: "
    "https://jupysql.ploomber.io/en/latest/api/configuration.html#named-parameters"
)


class Parameters(TraitType):
    def __init__(self, **kwargs):
        super(Parameters, self).__init__(**kwargs)

    def validate(self, obj, value):
        if isinstance(value, bool):
            if value:
                warnings.warn(
                    "named_parameters: boolean values are now deprecated. "
                    f'Value {value} will be treated as "enabled". \n'
                    f"{VALUE_WARNING}",
                    FutureWarning,
                )
                return "enabled"
            else:
                warnings.warn(
                    "named_parameters: boolean values are now deprecated. "
                    f'Value {value} will be treated as "warn" (default). \n'
                    f"{VALUE_WARNING}",
                    FutureWarning,
                )
                return "warn"
        elif isinstance(value, str):
            if not value:
                display.message(
                    'named_parameters: Value "" will be treated as "warn" (default)'
                )
                return "warn"

            value = value.lower()
            if value not in ("warn", "enabled", "disabled"):
                raise TraitError(
                    f"{value} is not a valid option for named_parameters. "
                    f'Valid options are: "warn", "enabled", or "disabled".'
                )

            return value

        else:
            raise TraitError(
                f"{value} is not a valid option for named_parameters. "
                f'Valid options are: "warn", "enabled", or "disabled".'
            )
