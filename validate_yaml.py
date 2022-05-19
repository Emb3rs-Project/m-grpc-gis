import yaml
import pydantic
from enum import Enum


class DependencyType(Enum):
    PIP = 1
    CONDA = 2


class Dependency(pydantic.BaseModel):
    package: str
    version: str
    type: DependencyType

    def __repr__(self):
        if self.type == DependencyType.CONDA:
            return f"{self.package}={self.version}"
        else:
            return f"{self.package}=={self.version}"


with open('environment-py39.yml') as f:
    data = yaml.load(f, Loader=yaml.CFullLoader)
    raw_dependencies = data.get('dependencies')
    dependencies: list[Dependency] = []

    for dependency in raw_dependencies:
        if type(dependency).__name__ != 'dict':
            d_package = dependency.split("=")[0]
            dependencies.append(Dependency(
                package=d_package,
                version=0,
                type=DependencyType.CONDA
            ))
        else:
            for pip_dependency in dependency["pip"]:
                [d_package, d_version] = pip_dependency.split("==")
                dependencies.append(Dependency(
                    package=d_package,
                    version=0,
                    type=DependencyType.PIP
                ))
    packages = list(map(lambda d: d.package, dependencies))
    dup = [x for i, x in enumerate(packages) if i != packages.index(x)]
    print(dup)
