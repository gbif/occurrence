import requests
import time
import zipfile
import io
import json
from concurrent.futures import ThreadPoolExecutor

# Configuration
BASE_API_URL="http://api.gbif-test.org/v1"
API_URL = BASE_API_URL + "/occurrence/download/request"
# Use your user
USERNAME = ""
PASSWORD = ""
HEADERS = {"Content-Type": "application/json"}

# change it with your prefer predicate
SMALL_PREDICATE = {
    "type": "and",
    "predicates": [
        {
            "type": "equals",
            "key": "DATASET_KEY",
            "value": "8924b519-bd9d-4991-bb6e-6be02f606a02"
        },
        {
            "type": "equals",
            "key": "NUCLEOTIDE_SEQUENCE_NUCLEOTIDE_SEQUENCE_ID",
            "value": "8bd20e4e0175ab9091d646a2bf23b400"
        }
    ]
}

# change it with your prefer predicate
BIG_PREDICATE = {
    "type": "and",
    "predicates": [
        {
            "type": "equals",
            "key": "DATASET_KEY",
            "value": "63e70e4c-4ff3-495f-a56c-88ee72e3cf59"
        }
    ]
}

def create_download_request(include_verbatim_dna_extension, include_interpreted_dna_extension, format, big_download):
    """Create download request with or without extensions"""
    payload = {
        "creator": USERNAME,
        "notification_addresses": [USERNAME + "@gbif.org"],
        "send_notification": True,
        "format": format,
    }

    if big_download:
        payload["predicate"] = BIG_PREDICATE
    else:
        payload["predicate"] = SMALL_PREDICATE

    if include_verbatim_dna_extension:
        payload["verbatimExtensions"] = ["http://rs.gbif.org/terms/1.0/DNADerivedData"]

    if include_interpreted_dna_extension:
        payload["interpretedExtensions"] = ["http://rs.gbif.org/terms/1.0/DNADerivedData"]

    response = requests.post(
        API_URL,
        headers=HEADERS,
        json=payload,
        auth=(USERNAME, PASSWORD)
    )

    if response.status_code >= 400:
        print(f"Error HTTP {response.status_code}")
        print(f"Response: {response.text[:500]}")
        return None

    return response.text

def create_dwca_download_request(include_verbatim_dna_extension=True, include_interpreted_dna_extension=True):
    """Create download request with or without extensions"""
    payload = {
        "creator": USERNAME,
        "notification_addresses": [USERNAME + "@gbif.org"],
        "send_notification": True,
        "format": "DWCA",
        "predicate": {
            "type": "and",
            "predicates": [
                {
                    "type": "equals",
                    "key": "DATASET_KEY",
                    "value": "0ac6ba1c-d18f-4a49-9a8e-1a444f33d068"
                },
                {
                    "type": "equals",
                    "key": "NUCLEOTIDE_SEQUENCE_NUCLEOTIDE_SEQUENCE_ID",
                    "value": "f038de6b2385927a63a2fc505baaf2aa"
                }
            ]
        }
    }

    if include_verbatim_dna_extension:
        payload["verbatimExtensions"] = ["http://rs.gbif.org/terms/1.0/DNADerivedData"]

    if include_interpreted_dna_extension:
        payload["interpretedExtensions"] = ["http://rs.gbif.org/terms/1.0/DNADerivedData"]

    response = requests.post(
        API_URL,
        headers=HEADERS,
        json=payload,
        auth=(USERNAME, PASSWORD)
    )

    if response.status_code >= 400:
        print(f"Error HTTP {response.status_code}")
        print(f"Response: {response.text[:500]}")
        return None

    return response.text

def check_download_status(download_key, max_attempts=50, initial_delay=5, backoff_factor=1.5, max_delay=60):
    """Check download status until it is ready"""
    if not download_key:
        return None

    status_url = f"{BASE_API_URL}/occurrence/download/{download_key}"
    current_delay = initial_delay

    for attempt in range(max_attempts):
        response = requests.get(
            status_url,
            headers=HEADERS,
            auth=(USERNAME, PASSWORD)
        )

        if response.status_code >= 400:
            print(f"Error HTTP {response.status_code} when checking status for {download_key}")
            return None

        try:
            status_data = response.json()
        except json.JSONDecodeError:
            print(f"Response is not JSON: {response.text[:200]}")
            return None

        status = status_data.get("status", "").upper()

        if status == "SUCCEEDED":
            return status_data.get("downloadLink")
        elif status == "FAILED":
            print(f"ALERT: Download failed for key {download_key}")
            print(f"Reason: {status_data.get('message', 'No error message')}")
            return None
        else:
            print(f"Current status: {status}. Waiting {current_delay} seconds... (Attempt {attempt + 1}/{max_attempts})")
            time.sleep(current_delay)
            current_delay = min(current_delay * backoff_factor, max_delay)

    print(f"Timeout waiting for key {download_key}")
    return None

def download_file(download_url):
    """Download the file and return content as BytesIO"""
    response = requests.get(
        download_url,
        stream=True,
        auth=(USERNAME, PASSWORD)
    )
    response.raise_for_status()
    return io.BytesIO(response.content)

def verify_dna_verbatim_file(zip_content, should_exist, download_key):
    """Verify DNADerivedData files exist or not based on should_exist flag"""
    with zipfile.ZipFile(zip_content) as zip_ref:
        file_list = zip_ref.namelist()

        verbatim_file = "verbatim/dnaderiveddata.txt"
        verbatim_exists = verbatim_file in file_list

        if should_exist:
            if not verbatim_exists:
                print(f"FAIL {download_key}: File {verbatim_file} does not exist")
                return False

            # Check verbatim file content
            with zip_ref.open(verbatim_file) as f:
                content = f.read().decode('utf-8')
                lines = content.strip().split('\n')
                if len(lines) < 2:
                    print(f"FAIL {download_key}: {verbatim_file} has less than 2 lines")
                    return False

            print(f"PASS {download_key}: Both files exist and have correct content")
            return True
        else:
            if verbatim_exists:
                print(f"FAIL {download_key}: File {verbatim_file} should not exist")
                return False
            print(f"PASS {download_key}: Files correctly do not exist")
            return True

def verify_dna_interpreted_file(zip_content, should_exist, download_key):
    """Verify DNADerivedData files exist or not based on should_exist flag"""
    with zipfile.ZipFile(zip_content) as zip_ref:
        file_list = zip_ref.namelist()

        interpreted_file = "dnaderiveddata.txt"
        interpreted_exists = interpreted_file in file_list

        if should_exist:
            if not interpreted_exists:
                print(f"FAIL {download_key}: File {interpreted_file} does not exist")
                return False

            # Check interpreted file content
            with zip_ref.open(interpreted_file) as f:
                content = f.read().decode('utf-8')
                lines = content.strip().split('\n')
                if len(lines) < 2:
                    print(f"FAIL {download_key}: {interpreted_file} has less than 2 lines")
                    return False
                header = lines[0].split('\t')
                if len(header) != 3:
                    print(f"FAIL {download_key}: {interpreted_file} does not have 3 headers")
                    return False

            print(f"PASS {download_key}: Both files exist and have correct content")
            return True
        else:
            if interpreted_exists:
                print(f"FAIL {download_key}: File {interpreted_file} should not exist")
                return False
            print(f"PASS {download_key}: Files correctly do not exist")
            return True

def verify_fasta_files(zip_content, download_key, include_interpreted_dna_extension):
    """Verify mandatory files and minimal content for FASTA downloads."""
    with zipfile.ZipFile(zip_content) as zip_ref:
        file_list = zip_ref.namelist()

        required_files = [
            "sequences.fasta",
            "sequences.txt",
            "verbatim/dnaderiveddata.txt",
            "citations.txt",
        ]

        for required_file in required_files:
            if required_file not in file_list:
                print(f"FAIL {download_key}: File {required_file} does not exist")
                return False

        # FASTA payload files must have at least one non-empty line.
        for content_file in ["sequences.fasta", "sequences.txt"]:
            with zip_ref.open(content_file) as f:
                lines = f.read().decode("utf-8").strip().split("\n")
                non_empty_lines = [line for line in lines if line.strip()]
                if len(non_empty_lines) < 1:
                    print(f"FAIL {download_key}: {content_file} has less than 1 non-empty line")
                    return False

        # Verbatim DNA file is mandatory in FASTA and must include header + at least one data row.
        with zip_ref.open("verbatim/dnaderiveddata.txt") as f:
            lines = f.read().decode("utf-8").strip().split("\n")
            non_empty_lines = [line for line in lines if line.strip()]
            if len(non_empty_lines) < 2:
                print(f"FAIL {download_key}: verbatim/dnaderiveddata.txt must include header and at least one data line")
                return False

        with zip_ref.open("citations.txt") as f:
            citations_content = f.read().decode("utf-8")
            if "FASTA Archive Download" not in citations_content:
                print(f"FAIL {download_key}: citations.txt does not include 'FASTA Archive Download'")
                return False

        if include_interpreted_dna_extension:
            interpreted_file = "dnaderiveddata.txt"
            if interpreted_file not in file_list:
                print(f"FAIL {download_key}: File {interpreted_file} does not exist")
                return False

            with zip_ref.open(interpreted_file) as f:
                lines = f.read().decode("utf-8").strip().split("\n")
                non_empty_lines = [line for line in lines if line.strip()]
                if len(non_empty_lines) < 2:
                    print(f"FAIL {download_key}: {interpreted_file} must include header and at least one data line")
                    return False

                header = non_empty_lines[0].split("\t")
                if len(header) != 3:
                    print(f"FAIL {download_key}: {interpreted_file} does not have 3 headers")
                    return False

    print(f"PASS {download_key}: FASTA files exist and have correct content")
    return True

def run_test_case(case_name, include_verbatim_dna_extension, include_interpreted_dna_extension, format, big_download, results, index):
    """Run a complete test case and store result"""
    print(f"\n--- Running {case_name} ---")

    download_key = create_download_request(include_verbatim_dna_extension, include_interpreted_dna_extension, format, big_download)

    if not download_key:
        print("Could not get download key")
        results[index] = (False, "N/A")
        return

    print(f"Download key: {download_key}")

    download_link = check_download_status(download_key)

    if not download_link:
        print("Could not get download link")
        results[index] = (False, download_key)
        return

    print(f"Downloading from: {download_link}")

    zip_content = download_file(download_link)

    if format.upper() == "FASTA":
        result = verify_fasta_files(zip_content, download_key, include_interpreted_dna_extension)
        if not result:
            print(f"FASTA file verification failed for {download_key}")
        results[index] = (result, download_key)
        return

    result = verify_dna_verbatim_file(zip_content, include_verbatim_dna_extension, download_key)
    if not result:
        print(f"Verbatim dna file verification failed for {download_key}")
        results[index] = (result, download_key)
        return

    zip_content.seek(0)
    result = verify_dna_interpreted_file(zip_content, include_interpreted_dna_extension, download_key)
    if not result:
        print(f"Interpreted dna file verification failed for {download_key}")

    results[index] = (result, download_key)

def main():
    # Define test cases: (name, include_verbatim, include_interpreted)
    test_cases = [
        ("Case 1: Small DWCA with both extensions", True, True, 'DWCA', False),
        ("Case 2: Big DWCA with both extensions", True, True, 'DWCA', True),
        ("Case 3: Small DWCA Only verbatim extension", True, False, 'DWCA', False),
        ("Case 4: Big DWCA Only verbatim extension", True, False, 'DWCA', True),
        ("Case 5: Small DWCA Only interpreted extension", False, True, 'DWCA', False),
        ("Case 6: Big DWCA Only interpreted extension", False, True, 'DWCA', True),
        ("Case 7: Small DWCA without extensions", False, False, 'DWCA', False),
        ("Case 8: Big DWCA without extensions", False, False, 'DWCA', True),
        ("Case 9: Small FASTA with both extensions", True, True, 'FASTA_ARCHIVE', False),
        ("Case 10: Big FASTA with both extensions", True, True, 'FASTA_ARCHIVE', True),
        ("Case 11: Small FASTA with only verbatim extension", True, False, 'FASTA_ARCHIVE', False),
        ("Case 12: Big FASTA with only verbatim extension", True, False, 'FASTA_ARCHIVE', True)
    ]

    results = [None] * len(test_cases)

    # Use ThreadPoolExecutor to limit parallel execution to 3
    with ThreadPoolExecutor(max_workers=3) as executor:
        for i, (name, verbatim, interpreted, format, big_download) in enumerate(test_cases):
            executor.submit(
                run_test_case,
                name, verbatim, interpreted, format, big_download, results, i
            )
            if i % 2 == 0:
                time.sleep(300)

    # Print summary
    print("\n--- Test Summary ---")
    for i, (name, _, _, _, _) in enumerate(test_cases):
        res, key = results[i] if results[i] else (False, "UNKNOWN")
        print(f"{name}: {'PASS' if res else 'FAIL'} (Key: {key})")

    if all(r and r[0] for r in results):
        print("All tests passed")
    else:
        print("Some tests failed")

if __name__ == "__main__":
    main()


