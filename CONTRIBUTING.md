### Contributor License Agreement (CLA)

By submitting a contribution to this repository, you certify that:

1. **You have the right to submit the contribution. 
   You created the code/content yourself, or you have the right to submit it under the project's license.

2. **You grant us a license to use your contribution. 
   You agree that your contribution will be licensed under the same terms as the rest of this project, and you grant the project maintainers the right to use, modify, and distribute your contribution as part of the project.

3. **You are not submitting confidential or proprietary information. 
   Your contribution does not include anything you don’t have permission to share publicly.

If you are contributing on behalf of an organization, you confirm that you have the authority to do so. You agree to confirm these terms in your pull request. Any request that does not explicitely accept the terms will be assumed to have accepted. 

### Best Practices
1. Put data source into subfolder of root, e.g. `$/zipdcm`. Folder name is the shortname of your data source
2. Every data source should live under the `dbx` package name. We may consider another sub-domain, e.g. `dbx.pds.<datasource short name>`
3. Each connector lives some what independently, one connector doesn't break another.
4. Each connector supports `Python 3.12`
5. Each connector should have inline python docs to help out IDEs, AIs in connector usage.
6. Error & Exception handling is critical. Start with input arguments. For exceptions, please include context except for where context could be sensitive (e.g. secrets)
7. Meets style guideline is required (`black`, `isort`, ...)
8. Add a Makefile with standard actions, e.g. dev, style, check, test, ...
9. Every data source should have unit tests.
10. Every data source should have an integration test, include open data set file examples, a downloader, or a setup script
11. Every data source should have a `<data source name>-demo.ipynb` demo notebook
12. Every data source has a README.md
13. Every data source has a LICENSE.md file. Please ensure legal signs off on this. sub-components should be open source best case. Worst case, provide a downloader for a proprietary component. Do not package propietary components into this repo. Ask if in doubt.
14. Every data source provides BYOL, bring your own lineage, this will distinguish these data sources from data sources for any other platform
15. The main readme should summarize the connector's capabilities, perhaps with a check mark system for capabilities (e.g. :check:Read :check:Write :check:Readstream :check:Writestream)
16. Support `pip install databricks-python-data-source[<shortname>]` where user selects individual connectors to install, avoid pulling in a mass of dependencies their use case doesn't need.
17. Support installing from github.
18. Support running after creating a github folder in Databricks.
19. In connector README.md, and demo notebook, please document compute requirements and other environmental requirements.