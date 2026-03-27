## Collaborative OPen Omics (COPO) Project

The Collaborative OPen Omics (COPO) project is an open-source web-based platform that enables scientists to describe their research objects (e.g. raw or processed data, assemblies, reads, samples and images) using community-sanctioned metadata sets and vocabularies.

As a metadata broker, COPO encourages scientists to submit metadata that complies with the Findable, Accessible, Interoperable and Reusable (FAIR) principles. These research objects are then shared with the wider scientific community via public repositories. The COPO project is based at the Earlham Institute in Norwich, England, United Kingdom.

This repository builds on the work of the [COPO](https://github.com/collaborative-open-plant-omics/COPO) GitHub repository (now archived), which laid the foundation for the current implementation.

## Tech stack overview

| Category                | Tools / Libraries                                 | Purpose / Usage                                             |
| ----------------------- | ------------------------------------------------- | ----------------------------------------------------------- |
| Backend Development     | Python, Django                                    | Core application logic and web framework                    |
| Scripting & Automation  | Python, Bash                                      | Automation, setup scripts, and server-side utilities        |
| Web framework           | Django (5.x), Django REST Framework               | Back-end web development, RESTful API design                |
| Task queue & scheduling | Celery, Redis, aioredis                           | Background task processing, caching, async queues           |
| Database                | PostgreSQL, MongoDB                               | Persistent data storage and querying                        |
| Deployment & serving    | Docker, Docker Swarm, Gunicorn, Nginx             | Production-ready app serving and containerisation           |
| Frontend technologies   | JavaScript (server-side), jQuery                  | Dynamic client-side behaviour and lightweight interactivity |
| Testing & QA            | pytest, Selenium, CircleCI, Django Test Framework | Automated testing and browser interaction validation        |
| DevOps / CI/CD          | GitHub Actions (CI/CD), Git                       | Version control and automated deployment pipelines          |
| Data handling           | Pandas, NumPy, openpyxl / XlsxWriter              | Data manipulation and scientific computing                  |
| File storage            | Django Storages, S3, MinIO, ECS (deprecated)      | Cloud-based file storage and management                     |
| Document generation     | Sphinx, ReadTheDocs                               | Auto-generated user documentation                           |
| HTML parsing / Scraping | BeautifulSoup, bs4                                | HTML content parsing, web scraping                          |
| User authentication     | django-allauth                                    | OAuth, OpenID, and account authentication                   |
| Real-time features      | Channels, Daphne                                  | WebSocket support and async communication                   |
| Styling & forms         | django-crispy-forms, crispy-bootstrap5            | Form rendering and UI styling                               |

## Frontend styling

The frontend uses multiple UI frameworks (Bootstrap 3, Semantic UI, Ace, DataTables, Select2, etc.). A single override stylesheet unifies their appearance.

### `static/copo/css/copo_modern_scheme.css`

This is a **drop-in cosmetic override** loaded as the last stylesheet in `<head>`. It does not add structural or layout styles — it re-themes every UI framework to produce one consistent look. Removing it should leave the app functional but unstyled.

**What belongs here:**
- Colour, typography, and spacing overrides for third-party frameworks
- CSS custom properties (design tokens) in `:root`
- Dark theme variants under `[data-theme="dark"]`
- Per-profile-type accent colours and tinted card headings

**What does NOT belong here:**
- Layout or structural styles for new components (use `style.css` or a component-specific stylesheet)
- Styles that the app depends on to function (e.g. visibility toggling, grid layout)

**Dark theme:** The `[data-theme="dark"]` block redefines CSS variables and adds targeted overrides for elements that use hardcoded colours. When adding a new themed rule, prefer using existing `--co-*` variables so the dark theme inherits it automatically. Only add a `[data-theme="dark"]` override when a variable alone isn't sufficient.

## Component icons and buttons

Component buttons (Samples, Reads, Assembly, etc.) are configured in the database via the `Component` model and populated by the `setup_profile_types` management command.

### Icon fields on `Component`

| Field | Purpose | Example |
|-------|---------|---------|
| `widget_icon` | Semantic UI icon name (legacy fallback) | `lab`, `dna`, `barcode` |
| `widget_icon_class` | Font Awesome class | `fa fa-vial`, `fa fa-microscope` |
| `material_icon` | Google Material Symbols name (preferred) | `labs`, `genetics`, `barcode` |
| `widget_colour` | Semantic UI colour class for the button | `olive`, `orange`, `red` |
| `button_label` | Short label shown on the button | `Samples`, `Reads`, `Barcoding` |

### How icons are resolved

The frontend (`generic_handlers.js` → `setComponentIcon()` / `createComponentAnchor()`) checks `materialIcon` first. If set, it renders a Material Symbol. Otherwise, it falls back to the Semantic UI icon (`semanticIcon`).

### Adding or changing a component icon

1. Edit `src/apps/copo_core/management/commands/setup_profile_types.py`
2. Update the `create_component()` call with the new `material_icon`, `widget_icon_class`, or `button_label`
3. Run `python manage.py setup_profile_types` on the target instance

### Migration note

The `material_icon` field was added in migration `0027_add_material_icon_to_component`. Instances that have not yet run `setup_profile_types` will have `material_icon = NULL`, which is safe — the frontend falls back to the Semantic UI icon. Once the command is run, Material Icons take effect.

### Other stylesheets

| File | Purpose |
|------|---------|
| `style.css` | Legacy base styles — structural layout, component positioning |
| `*_style.css` / `*.css` (component-specific) | Styles scoped to individual features (e.g. `browse_style.css`, `copo_wizard.css`) |

## Related repositories

- [COPO-production](https://github.com/EarlhamInst/COPO-production) – _This repository (included for reference)_
- [COPO-schemas](https://github.com/EarlhamInst/COPO-schemas)
- [COPO-documentation](https://github.com/EarlhamInst/COPO-documentation)
- [COPO-sample-audit](https://github.com/EarlhamInst/COPO-sample-audit)
- [SingleCellSchemas](https://github.com/EarlhamInst/SingleCellSchemas)

## Additional resources

- [General documentation about the COPO project](https://copo-docs.readthedocs.io/en/latest)

- [Steps to setting up the COPO project locally](https://copo-docs.readthedocs.io/en/latest/advanced/project_setup/project-local-setup-index.html)

- [Deployment guidelines](https://copo-docs.readthedocs.io/en/latest/advanced/project_setup/project-local-setup-index.html#deploy-docker-image-on-docker-swarm-manager)

- [Guidelines for configuring profile types](https://copo-docs.readthedocs.io/en/latest/advanced/profile_setup/profile-setup-index.html)
- [![DOI](https://zenodo.org/badge/31064842.svg)](https://zenodo.org/badge/latestdoi/31064842)
- [COPO's FAIRsharing resource](https://doi.org/10.25504/FAIRsharing.91a79b)

- [Single-cell website](https://singlecellschemas.org/)

- [To report issues](https://github.com/EarlhamInst/COPO-production/issues)
