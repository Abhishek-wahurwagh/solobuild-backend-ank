from fastapi import FastAPI, UploadFile, File
from fastapi.openapi.utils import get_openapi

app = FastAPI()

@app.post("/test1")
def test1(files: list[UploadFile] = File(...)):
    pass

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Custom title",
        version="2.5.0",
        description="This is a very custom OpenAPI schema",
        routes=app.routes,
    )
    for path in openapi_schema["paths"].values():
        for method in path.values():
            if "requestBody" in method:
                content = method["requestBody"]["content"]
                if "multipart/form-data" in content:
                    schema = content["multipart/form-data"]["schema"]
                    if "$ref" in schema:
                        ref_name = schema["$ref"].split("/")[-1]
                        # We need to find the actual schema in components
                        comp = openapi_schema["components"]["schemas"][ref_name]
                        for prop_name, prop_val in comp["properties"].items():
                            if prop_val.get("type") == "array" and prop_val.get("items", {}).get("type") == "string":
                                # Swagger UI 5.x workaround
                                prop_val["items"]["format"] = "binary"
                                prop_val["items"].pop("contentMediaType", None)

    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

if __name__ == "__main__":
    import json
    print(json.dumps(app.openapi(), indent=2))
