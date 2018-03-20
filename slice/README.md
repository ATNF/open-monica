Usage
-----

For each new ice version interfaces need to be generated and compile.
Add the version-numbered upstream ice jars to the directory `../ice`.
Make sure tha `slice2java --version` matches this.

Change the property `ice-version` in the _build.xml_ file to match then run `ant` in 
this directory. This will generate _../ice/open-monica-ice-${ice-version}.jar_.

After this add all new jars in _../ice_ to the repository.

The build/runtime usage of open-monica is controlled via `ice-version` in the project _build.xml_.
The matching ice jats will be copied into _3rdParty_ as part of the `compile` task.
