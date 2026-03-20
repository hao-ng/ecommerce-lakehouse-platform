import requests


class SchemaRegistryClient:
    def __init__(self, registry_url):
        self.registry_url = registry_url

    def get_schema(self, subject) -> str:
        """Fetch schema from the registry for a given subject

        Args:
            subject (str): Subject name in the schema registry

        Returns:
            str: Schema in JSON string format
        """
        response = requests.get(
            f"{self.registry_url}/subjects/{subject}/versions/latest"
        )
        if response.status_code == 200:
            return response.json()["schema"]
        else:
            raise Exception(
                f"Failed to fetch schema for subject {subject}: {response.text}"
            )
