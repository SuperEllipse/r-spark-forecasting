# File: install_packages.R

# Read packages from requirements.txt
packages <- readLines("src/requirements.txt")

# Function to install missing packages
install_if_missing <- function(package) {
  if (!requireNamespace(package, quietly = TRUE)) {
    install.packages(package, dependencies = TRUE)
  }
}

# Install missing packages
sapply(packages, install_if_missing)

# Load all packages
lapply(packages, library, character.only = TRUE)

print("Setup complete. All required packages are installed and loaded.")