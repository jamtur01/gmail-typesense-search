# Gmail-Typesense Search Application

![Email Search](./assets/email-search-banner.png)

Welcome to the **Gmail-Typesense Search Application**, a powerful tool that enables efficient searching and indexing of your email data using Python for backend processing and Svelte for the frontend interface. Leveraging **Typesense** and **InstantSearch.js**, this application offers a seamless and responsive search experience.

## Table of Contents

- [Gmail-Typesense Search Application](#gmail-typesense-search-application)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
    - [Backend](#backend)
    - [Frontend](#frontend)
    - [Database](#database)
  - [Project Structure](#project-structure)
  - [Backend Setup](#backend-setup)
    - [1. Clone the Repository](#1-clone-the-repository)
    - [2. Set Up Python Environment](#2-set-up-python-environment)
    - [3. Install Python Dependencies](#3-install-python-dependencies)
    - [4. Configure Typesense](#4-configure-typesense)
      - [4.1 Running Typesense with Docker](#41-running-typesense-with-docker)
        - [**Prerequisites**](#prerequisites-1)
        - [**Steps to Run Typesense with Docker**](#steps-to-run-typesense-with-docker)
        - [**Additional Docker Tips**](#additional-docker-tips)
        - [**Environment Variables for Docker**](#environment-variables-for-docker)
  - [5. Create the schema](#5-create-the-schema)
  - [6. Index Your Email Data](#6-index-your-email-data)
  - [Frontend Setup](#frontend-setup)
    - [1. Navigate to Frontend Directory](#1-navigate-to-frontend-directory)
    - [2. Install Node.js and npm](#2-install-nodejs-and-npm)
    - [3. Install Frontend Dependencies](#3-install-frontend-dependencies)
    - [4. Configure Frontend](#4-configure-frontend)
    - [5. Start the Frontend Server](#5-start-the-frontend-server)
  - [Usage](#usage)
  - [Troubleshooting](#troubleshooting)
    - [Common Issues](#common-issues)
    - [Additional Tips](#additional-tips)
  - [Contributing](#contributing)
  - [Additional Notes](#additional-notes)
  - [License](#license)

## Features

- **Efficient Search:** Quickly search through large volumes of email data with instant results.
- **Vector and Normal Modes:** Toggle between traditional keyword search and semantic vector-based search for more nuanced results.
- **Responsive Frontend:** Built with Svelte for a fast and reactive user interface.
- **Powerful Backend:** Python scripts handle data indexing and interaction with Typesense.
- **Real-time Suggestions:** Get relevant search suggestions as you type.

## Prerequisites

Before setting up the project, ensure you have the following installed on your system:

### Backend

- **Python 3.8+**
- **pip** (Python package installer)
- **Virtualenv** (optional but recommended)

### Frontend

- **Node.js 14+**
- **npm** (Node package manager)

### Database

- **Typesense Server** (Self-hosted or Typesense Cloud)

## Project Structure

```
email-search-app/
├── backend/
│   ├── index_mbox_typesense.py
│   ├── setup_typesense.py
│   ├── requirements.txt
│   └── .env
├── frontend/
│   ├── public/
│   ├── src/
│   ├── package.json
│   ├── svelte.config.js
│   └── .env
├── README.md
└── ... (other root files)
```

## Backend Setup

### 1. Clone the Repository

```bash
git clone https://github.com/jamtur01/email-search-app.git
cd email-search-app
```

### 2. Set Up Python Environment

It's recommended to use a virtual environment to manage Python dependencies.

```bash
# Navigate to the backend directory
cd backend

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
# On Unix or MacOS
source venv/bin/activate

# On Windows
venv\Scripts\activate
```

### 3. Install Python Dependencies

Install the required Python packages using `pip`.

```bash
pip install -r requirements.txt
```

### 4. Configure Typesense

Ensure you have a running Typesense server. You can either self-host Typesense or use [Typesense Cloud](https://cloud.typesense.org/).

#### 4.1 Running Typesense with Docker

Running Typesense using Docker is a straightforward way to set up the server without worrying about manual installations. Follow the steps below to get Typesense up and running using Docker.

##### **Prerequisites**

- **Docker:** Ensure that Docker is installed on your system. You can download and install Docker from the [official website](https://www.docker.com/get-started).

##### **Steps to Run Typesense with Docker**

1. **Pull the Typesense Docker Image**

   ```bash
   docker pull typesense/typesense:0.27.1
   ```

   _Note:_ Replace `0.27.1` with the latest version if available.

2. **Run the Typesense Server**

   ```bash
   docker run -p 8108:8108 \
     -v $(pwd)/data:/data \
     typesense/typesense:0.27.1 \
     --data-dir=/data \
     --api-key=abc123 \
     --enable-cors=true
   ```

   **Explanation of the Command:**

   - `docker run`: Runs a new Docker container.
   - `-p 8108:8108`: Maps port `8108` of the container to port `8108` on your host machine. Ensure that this port is not occupied by another service.
   - `-v $(pwd)/data:/data`: Mounts the `data` directory in your current working directory to `/data` inside the container. This ensures that your Typesense data persists even if the container is restarted or removed.
   - `typesense/typesense:0.27.1`: Specifies the Docker image and tag to use.
   - `--data-dir=/data`: Sets the directory where Typesense will store its data inside the container.
   - `--api-key=abc123`: Sets the API key for Typesense. **Replace `abc123` with your own secure API key.**
   - `--enable-cors=true`: Enables Cross-Origin Resource Sharing (CORS), allowing your frontend application to communicate with Typesense without CORS issues.

3. **Insert Your Own Keys**

   - **API Key:** Replace `abc123` with a secure API key of your choice. This key will be used by your frontend application to authenticate requests to Typesense.

   - **OpenAI API Key (if applicable):** If your application integrates with OpenAI services, replace `sk-OPENAI_KEY_HERE` with your actual OpenAI API key. Ensure that this key is kept secure and not exposed publicly.

   **Example Command with Your Own Keys:**

   ```bash
   docker run -p 8108:8108 \
     -v $(pwd)/data:/data \
     typesense/typesense:0.27.1 \
     --data-dir=/data \
     --api-key=your_secure_api_key \
     --openai-api-key=sk-your_openai_key_here \
     --enable-cors=true
   ```

4. **Verify Typesense is Running**

   Once the container is up and running, you can verify by sending a request to the Typesense server.

   ```bash
   curl -X GET "http://localhost:8108/health"
   ```

   **Expected Response:**

   ```json
   {
     "ok": true
   }
   ```

5. **Persisting Data**

   The `-v $(pwd)/data:/data` flag ensures that all Typesense data is stored in the `data` directory within your project. This setup allows data persistence across container restarts or recreations.

6. **Stopping the Typesense Container**

   To stop the running Typesense container, first find the container ID or name:

   ```bash
   docker ps
   ```

   Then stop it using:

   ```bash
   docker stop <container_id_or_name>
   ```

##### **Additional Docker Tips**

- **Running in Detached Mode:**

  To run Typesense in the background without attaching to the terminal, add the `-d` flag.

  ```bash
  docker run -d -p 8108:8108 \
    -v $(pwd)/data:/data \
    typesense/typesense:0.27.1 \
    --data-dir=/data \
    --api-key=your_secure_api_key \
    --enable-cors=true
  ```

- **Removing the Container:**

  To remove a stopped container, use:

  ```bash
  docker rm <container_id_or_name>
  ```

- **Updating Typesense:**

  To update Typesense to a newer version, pull the latest image and rerun the container.

  ```bash
  docker pull typesense/typesense:latest
  docker run -d -p 8108:8108 \
    -v $(pwd)/data:/data \
    typesense/typesense:latest \
    --data-dir=/data \
    --api-key=your_secure_api_key \
    --enable-cors=true
  ```

##### **Environment Variables for Docker**

Alternatively, you can use environment variables to pass configurations, which can make managing secrets easier.

```bash
docker run -d -p 8108:8108 \
  -v $(pwd)/data:/data \
  -e TYPESENSE_API_KEY=your_secure_api_key \
  -e OPENAI_API_KEY=sk-your_openai_key_here \
  typesense/typesense:0.27.1 \
  --data-dir=/data \
  --enable-cors=true
```

## 5. Create the schema

Use the provided Python script to setup Typesense and create the schema.

```bash
python setup_typesense.py
```

## 6. Index Your Email Data

Use the provided Python script to index your email data into Typesense.

```bash
python index_mbox_typesense.py
```

## Frontend Setup

### 1. Navigate to Frontend Directory

```bash
cd frontend
```

### 2. Install Node.js and npm

If you haven't installed Node.js and npm, download and install them from the [official website](https://nodejs.org/).

Verify the installation:

```bash
node -v
npm -v
```

### 3. Install Frontend Dependencies

Install the necessary packages using `npm`.

```bash
npm install
```

### 4. Configure Frontend

Ensure that the frontend is correctly configured to communicate with your Typesense server.

**Configuration Steps:**

1. **Environment Variables:**

   Create a `.env` file in the `frontend/` directory.

   ```bash
   touch .env
   ```

   **`.env` Example:**

   ```plaintext
   VITE_TYPESENSE_API_KEY=abc123
   VITE_TYPESENSE_HOST=localhost
   VITE_TYPESENSE_PORT=8108
   VITE_TYPESENSE_PROTOCOL=http
   VITE_COLLECTION_NAME=emails
   ```

   **Important:** Replace `abc123` with your actual Typesense API key and ensure other variables match your Typesense server configuration.

### 5. Start the Frontend Server

Run the development server to start the frontend application.

```bash
npm run dev
```

By default, the application should be accessible at [http://localhost:5173](http://localhost:5173).

1. **Access the Application:**

   Open your browser and navigate to [http://localhost:5173](http://localhost:5173) to use the Gmail-Typesense Search Application.

## Usage

- **Search Emails:** Enter your search query in the search box. The application supports both **Normal Mode** (keyword-based search) and **Vector Mode** (semantic search).
- **Toggle Search Mode:** Use the toggle switch to switch between Normal and Vector modes. Hover over the toggle for more information.
- **View Results:** The search results will display below the search bar, showing the subject, sender, date, and a snippet of the email body.
- **Pagination:** Navigate through the search results using the pagination controls at the bottom of the results.

## Troubleshooting

### Common Issues

1. **Typesense Server Not Running:**

   - **Symptom:** Frontend cannot communicate with Typesense; no search results.
   - **Solution:** Ensure that the Typesense server is running and accessible. Check the server logs for errors.

2. **CORS Errors:**

   - **Symptom:** Browser console shows CORS policy errors.
   - **Solution:** Ensure that the `--enable-cors=true` flag is set when running Typesense. Additionally, verify that the frontend's origin is allowed if you have specific CORS configurations.

3. **Environment Variables Not Loaded:**

   - **Symptom:** Application cannot find Typesense configurations.
   - **Solution:** Ensure that `.env` files are correctly set up in both `backend/` and `frontend/` directories and that they are loaded properly. Restart the servers after making changes to `.env` files.

4. **Indexing Errors:**

   - **Symptom:** Python script fails to index documents.
   - **Solution:** Verify that the Typesense server is running and that the API key and collection names match the configuration. Check the Python script for any errors in parsing the `.mbox` file.

### Additional Tips

- **Check Logs:** Both backend and frontend servers provide logs that can help identify issues. Review the terminal outputs for any error messages.
- **Network Issues:** Ensure that there are no firewall rules or network issues preventing communication between the frontend and Typesense server.
- **Dependencies:** Make sure all dependencies are correctly installed. Reinstalling packages can sometimes resolve unexpected issues.
- **Data Formatting:** Ensure that your email data is correctly formatted and that all required fields (`id`, `subject`, `sender`, `date`, `body`) are present and properly parsed.
- **Docker Permissions:** If running Typesense with Docker on Unix-based systems, ensure that you have the necessary permissions to bind ports and mount volumes.

## Contributing

Contributions are welcome! Please follow these steps to contribute:

1. **Fork the Repository**

2. **Create a Feature Branch**

   ```bash
   git checkout -b feature/YourFeatureName
   ```

3. **Commit Your Changes**

   ```bash
   git commit -m "Add your message here"
   ```

4. **Push to the Branch**

   ```bash
   git push origin feature/YourFeatureName
   ```

5. **Open a Pull Request**

   Describe your changes and submit a pull request for review.

## Additional Notes

- **Security:** Ensure that your API keys (`TYPESENSE_API_KEY` and `OPENAI_API_KEY`) are kept secure. Do not expose them in public repositories or client-side code.
- **Docker Volumes:** The `-v $(pwd)/data:/data` flag mounts the `data` directory from your current working directory to `/data` inside the Docker container. This ensures data persistence across container restarts.
- **Environment Variable Management:** Consider using tools like `dotenv` to manage environment variables more effectively, especially in production environments.
- **Scaling Typesense:** For larger datasets or higher traffic, consider scaling your Typesense deployment using orchestration tools like Kubernetes or leveraging Typesense Cloud for managed hosting.

If you have any further questions or need additional assistance with your project setup, feel free to ask!

## License

This project is licensed under the [MIT License](LICENSE).

---

_Developed with ❤️ by [James Turnbull](https://github.com/jamtur01)_
