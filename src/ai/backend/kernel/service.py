import json
import logging
from pathlib import Path
from typing import (
    Any,
    Collection,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
)

import attrs

from . import service_actions
from .exception import DisallowedArgument, DisallowedEnvironment, InvalidServiceDefinition
from .logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger())


class Action(TypedDict):
    action: str
    args: Mapping[str, str]
    ref: Optional[str]


@attrs.define(auto_attribs=True, slots=True)
class ServiceDefinition:
    command: List[str]
    noop: bool = False
    url_template: str = ""
    prestart_actions: List[Action] = attrs.Factory(list)
    env: Mapping[str, str] = attrs.Factory(dict)
    allowed_envs: List[str] = attrs.Factory(list)
    allowed_arguments: List[str] = attrs.Factory(list)
    default_arguments: Mapping[str, Union[None, str, List[str]]] = attrs.Factory(dict)


class ServiceParser:
    variables: MutableMapping[str, str]
    services: MutableMapping[str, ServiceDefinition]

    def __init__(self, variables: MutableMapping[str, str]) -> None:
        self.variables = variables
        self.services = {}

    async def parse(self, path: Path) -> None:
        for service_def_file in path.glob("*.json"):
            log.debug(f"loading service-definition from {service_def_file}")
            try:
                with open(service_def_file.absolute(), "rb") as fr:
                    raw_service_def = json.load(fr)
                    if "prestart" in raw_service_def:
                        raw_service_def["prestart_actions"] = raw_service_def["prestart"]
                        del raw_service_def["prestart"]
            except IOError:
                raise InvalidServiceDefinition(
                    f"could not read the service-def file: {service_def_file.name}"
                )
            except json.JSONDecodeError:
                raise InvalidServiceDefinition(
                    f"malformed JSON in service-def file: {service_def_file.name}"
                )
            name = service_def_file.stem
            try:
                self.services[name] = ServiceDefinition(**raw_service_def)
            except TypeError as e:
                raise InvalidServiceDefinition(e.args[0][11:])

    def add_model_service(self, name, model_service_info) -> None:
        service_def = ServiceDefinition(
            model_service_info["start_command"],
            prestart_actions=model_service_info["pre_start_actions"] or [],
        )
        self.services[name] = service_def

    async def start_service(
        self,
        service_name: str,
        frozen_envs: Collection[str],
        opts: Mapping[str, Any],
    ) -> Tuple[Optional[Sequence[str]], Mapping[str, str]]:
        if service_name not in self.services.keys():
            return None, {}
        service = self.services[service_name]
        if service.noop:
            return [], {}

        for action in service.prestart_actions:
            try:
                action_impl = getattr(service_actions, action["action"])
            except AttributeError:
                raise InvalidServiceDefinition(
                    f"Service-def for {service_name} used invalid action: {action['action']}"
                )
            ret = await action_impl(self.variables, **action["args"])
            if (ref := action.get("ref")) is not None:
                self.variables[ref] = ret

        cmdargs, env = [], {}

        for arg in service.command:
            cmdargs.append(arg.format_map(self.variables))

        for arg_name, arg_value in additional_arguments.items():
            cmdargs.append(arg_name)
            if isinstance(arg_value, str):
                cmdargs.append(arg_value)
            elif isinstance(arg_value, list):
                cmdargs += arg_value

        return cmdargs, env

    async def get_apps(self, selected_service: str = "") -> Sequence[Mapping[str, Any]]:
        def _format(service_name: str) -> Mapping[str, Any]:
            service_info: Dict[str, Any] = {"name": service_name}
            service = self.services[service_name]
            if len(service.url_template) > 0:
                service_info["url_template"] = service.url_template
            if len(service.allowed_arguments) > 0:
                service_info["allowed_arguments"] = service.allowed_arguments
            if len(service.allowed_envs) > 0:
                service_info["allowed_envs"] = service.allowed_envs
            return service_info

        apps = []
        if selected_service:
            if selected_service in self.services.keys():
                apps.append(_format(selected_service))
        else:
            for service_name in self.services.keys():
                apps.append(_format(service_name))
        return apps