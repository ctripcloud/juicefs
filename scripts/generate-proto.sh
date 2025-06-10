#!/bin/bash

# Script to generate protobuf code for JuiceFS
# This script can be used as an alternative to make proto

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if required tools are installed
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    if ! command -v protoc &> /dev/null; then
        print_error "protoc is not installed. Please install Protocol Buffers compiler."
        exit 1
    fi
    
    if ! command -v protoc-gen-go &> /dev/null; then
        print_error "protoc-gen-go is not installed. Running: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
        go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    fi
    
    if ! command -v protoc-gen-go-grpc &> /dev/null; then
        print_error "protoc-gen-go-grpc is not installed. Running: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
        go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    fi
    
    print_status "All prerequisites satisfied"
}

# Function to generate protobuf code
generate_proto() {
    print_status "Generating protobuf code..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
    PROTO_DIR="$PROJECT_ROOT/proto"
    OUTPUT_DIR="$PROJECT_ROOT/pkg/proxy/v1"
    
    # Create output directory if it doesn't exist
    mkdir -p "$OUTPUT_DIR"
    
    # Clean previous generated files
    rm -f "$OUTPUT_DIR"/*.pb.go
    
    # Generate Go protobuf code
    protoc \
        --proto_path="$PROTO_DIR" \
        --go_out="$OUTPUT_DIR" \
        --go_opt=paths=source_relative \
        --go-grpc_out="$OUTPUT_DIR" \
        --go-grpc_opt=paths=source_relative \
        "$PROTO_DIR"/*.proto
    
    print_status "Protobuf code generation completed successfully!"
    print_status "Generated files in: $OUTPUT_DIR"
}

# Function to validate generated code
validate_generated_code() {
    print_status "Validating generated code..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
    OUTPUT_DIR="$PROJECT_ROOT/pkg/proxy/v1"
    
    cd "$OUTPUT_DIR"
    if go build . &> /dev/null; then
        print_status "Generated code validation passed!"
    else
        print_error "Generated code validation failed!"
        exit 1
    fi
}

# Main execution
main() {
    print_status "Starting protobuf code generation for JuiceFS..."
    
    check_prerequisites
    generate_proto
    validate_generated_code
    
    print_status "All done! ✨"
}

# Run main function
main "$@" 