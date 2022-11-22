# nano-platform
Platform for storing objects, deploying small runtimes, and hosting small web pages.

*(Still a prototype)*

---

## Details

Easy to learn, lightweight, and fast "nanoservice" deployment platform.
The platform, via its API, provides an abstraction layer above different cloud services (AWS Lambda, S3) simplifying the processes of:
- storing objects/documents/datasets,
- computing runtimes,
- writing and hosting small web pages (via yaml description of the page),
with easy and fast deployment for those three REST resources.
This is a great tool for developers who want to focus on their code and not on the infrastructure.

## Getting Started
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run API
```bash
uvicorn main:app --reload
```
Should be run inside container/cluster as microservice.

## GUI
Does include a swagger GUI (openapi.json, following OAS3.0 specification) for the API.
May be interfaced with any other web frontend for convenience.

## License
MIT
