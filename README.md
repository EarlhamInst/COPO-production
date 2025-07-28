## Collaborative OPen Omics (COPO) Project

The Collaborative OPen Omics (COPO) project is an open-source web-based platform that enables scientists to describe their research objects (e.g. raw or processed data, assemblies, reads, samples and images) using community-sanctioned metadata sets and vocabularies.

As a metadata broker, COPO encourages scientists to submit metadata that complies with the Findable, Accessible, Interoperable and Reusable (FAIR) principles. These research objects are then shared with the wider scientific community via public repositories. The COPO project is based at the Earlham Institute in Norwich, England, United Kingdom.

This repository builds on the work of the [COPO](https://github.com/collaborative-open-plant-omics/COPO) GitHub repository (now archived), which laid the foundation for the current implementation.

## Tech stack overview

| Category                 | Tools / Libraries                                     | Purpose / Usage                                               |
|--------------------------|-------------------------------------------------------|---------------------------------------------------------------|
| Backend Development      | Python, Django                                        | Core application logic and web framework                      |
| Scripting & Automation   | Python, Bash                                          | Automation, setup scripts, and server-side utilities          |
| Web framework            | Django (5.x), Django REST Framework                   | Back-end web development, RESTful API design                  |
| Task queue & scheduling  | Celery, Redis, aioredis                               | Background task processing, caching, async queues             |
| Database                 | PostgreSQL, MongoDB                                   | Persistent data storage and querying                          |
| Deployment & serving     | Docker, Docker Swarm, Gunicorn, Nginx                 | Production-ready app serving and containerisation             |
| Frontend technologies    | JavaScript (server-side), jQuery                      | Dynamic client-side behaviour and lightweight interactivity   |
| Testing & QA             | pytest, Selenium, CircleCI, Django Test Framework     | Automated testing and browser interaction validation          |
| DevOps / CI/CD           | GitHub Actions (CI/CD), Git                           | Version control and automated deployment pipelines            |
| Data handling            | Pandas, NumPy, openpyxl / XlsxWriter                  | Data manipulation and scientific computing                    |
| File storage             | Django Storages, S3, MinIO, ECS (deprecated)          | Cloud-based file storage and management                       |
| Document generation      | Sphinx, ReadTheDocs                                   | Auto-generated user documentation                             |
| HTML parsing / Scraping  | BeautifulSoup, bs4                                    | HTML content parsing, web scraping                            |
| User authentication      | django-allauth                                        | OAuth, OpenID, and account authentication                     |
| Real-time features       | Channels, Daphne                                      | WebSocket support and async communication                     |
| Styling & forms          | django-crispy-forms, crispy-bootstrap5                | Form rendering and UI styling                                 |

## Related repositories

- [COPO-production](https://github.com/TGAC/COPO-production) – _This repository (included for reference)_
- [COPO-schemas](https://github.com/TGAC/COPO-schemas)
- [COPO-documentation](https://github.com/TGAC/COPO-documentation)
- [COPO-sample-audit](https://github.com/TGAC/COPO-sample-audit)
- [SingleCellSchemas](https://github.com/TGAC/SingleCellSchemas)

## Additional resources

- [General documentation about the COPO project](https://copo-docs.readthedocs.io/en/latest)

- [Steps to setting up the COPO project locally](https://copo-docs.readthedocs.io/en/latest/advanced/project_setup/project-local-setup-index.html)

- [Deployment guidelines](https://copo-docs.readthedocs.io/en/latest/advanced/project_setup/project-local-setup-index.html#deploy-docker-image-on-docker-swarm-manager)

- [Guidelines for configuring profile types](https://copo-docs.readthedocs.io/en/latest/advanced/profile_setup/profile-setup-index.html)
- [![DOI](https://zenodo.org/badge/31064842.svg)](https://zenodo.org/badge/latestdoi/31064842)
- [COPO's FAIRsharing resource](https://doi.org/10.25504/FAIRsharing.91a79b)

- [Single-cell website](https://singlecellschemas.org/)

- [To report issues](https://github.com/TGAC/COPO-production/issues)
