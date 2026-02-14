package reader

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// SchemaInfo represents metadata about a single column in a Parquet file.
type SchemaInfo struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	PhysicalType string `json:"physical_type"`
	LogicalType  string `json:"logical_type"`
	Required     bool   `json:"required"`
	Optional     bool   `json:"optional"`
	Repeated     bool   `json:"repeated"`
}

// ExtractSchemaInfo extracts schema information from a Parquet file.
//
// Returns a slice of SchemaInfo containing metadata about each column including
// name, type information, and whether the field is required/optional/repeated.
//
// For nested types, field names use dot notation (e.g., "address.street").
func ExtractSchemaInfo(path string) ([]SchemaInfo, error) {
	reader, err := NewReader(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer func() { _ = reader.Close() }()

	schema := reader.Schema()
	fields := schema.Fields()

	var schemaInfos []SchemaInfo
	for _, field := range fields {
		schemaInfos = append(schemaInfos, extractFieldInfo(field, "")...)
	}

	return schemaInfos, nil
}

// extractFieldInfo recursively extracts schema information from a field.
// The prefix parameter is used to build dot-notation names for nested fields.
func extractFieldInfo(field parquet.Field, prefix string) []SchemaInfo {
	return extractFieldInfoWithParentRepeated(field, prefix, false)
}

// extractFieldInfoWithParentRepeated recursively extracts schema information from a field,
// tracking whether any parent field is repeated.
func extractFieldInfoWithParentRepeated(field parquet.Field, prefix string, parentRepeated bool) []SchemaInfo {
	var infos []SchemaInfo

	// Build field name with prefix for nested fields
	fieldName := field.Name()
	if prefix != "" {
		fieldName = prefix + "." + fieldName
	}

	// Check if this field or any parent is repeated
	isRepeated := parentRepeated || field.Repeated()

	// Check if field has child fields (nested type/group)
	childFields := field.Fields()
	if len(childFields) > 0 {
		// This is a group/struct - recurse into child fields
		// Don't create a SchemaInfo for the group itself, only for leaf fields
		// Propagate repeated status to children
		for _, child := range childFields {
			infos = append(infos, extractFieldInfoWithParentRepeated(child, fieldName, isRepeated)...)
		}
		return infos
	}

	// This is a leaf field - extract type information
	physicalType := getPhysicalType(field)
	logicalType := getLogicalType(field)
	userType := getUserFriendlyType(field)

	// Extract repetition information
	required := field.Required()
	optional := field.Optional()

	info := SchemaInfo{
		Name:         fieldName,
		Type:         userType,
		PhysicalType: physicalType,
		LogicalType:  logicalType,
		Required:     required,
		Optional:     optional,
		Repeated:     isRepeated,
	}

	infos = append(infos, info)

	return infos
}

// getPhysicalType returns the physical type name of a Parquet field.
func getPhysicalType(field parquet.Field) string {
	if field.Type() == nil {
		return "GROUP"
	}

	kind := field.Type().Kind()
	switch kind {
	case parquet.Boolean:
		return "BOOLEAN"
	case parquet.Int32:
		return "INT32"
	case parquet.Int64:
		return "INT64"
	case parquet.Int96:
		return "INT96"
	case parquet.Float:
		return "FLOAT"
	case parquet.Double:
		return "DOUBLE"
	case parquet.ByteArray:
		return "BYTE_ARRAY"
	case parquet.FixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		return "UNKNOWN"
	}
}

// getLogicalType returns the logical type name of a Parquet field.
func getLogicalType(field parquet.Field) string {
	if field.Type() == nil {
		return ""
	}

	logicalType := field.Type().LogicalType()
	if logicalType == nil {
		return ""
	}

	// Use String() method which provides the logical type name
	return logicalType.String()
}

// getUserFriendlyType returns a user-friendly type name for a Parquet field.
//
// This converts Parquet's physical and logical types into simpler, more
// recognizable type names for end users.
func getUserFriendlyType(field parquet.Field) string {
	if field.Type() == nil {
		return "GROUP"
	}

	// Check logical type first for more specific typing
	logicalType := field.Type().LogicalType()
	if logicalType != nil {
		logicalTypeStr := logicalType.String()
		switch logicalTypeStr {
		case "STRING", "UTF8":
			return "STRING"
		case "ENUM":
			return "ENUM"
		case "UUID":
			return "UUID"
		case "INT":
			// For INT logical type, check the physical type
			kind := field.Type().Kind()
			switch kind {
			case parquet.Int32:
				return "INT32"
			case parquet.Int64:
				return "INT64"
			}
		case "DATE":
			return "DATE"
		case "TIME":
			return "TIME"
		case "TIMESTAMP":
			return "TIMESTAMP"
		case "DECIMAL":
			return "DECIMAL"
		case "JSON":
			return "JSON"
		case "BSON":
			return "BSON"
		}
	}

	// Fall back to physical type
	kind := field.Type().Kind()
	switch kind {
	case parquet.Boolean:
		return "BOOLEAN"
	case parquet.Int32:
		return "INT32"
	case parquet.Int64:
		return "INT64"
	case parquet.Int96:
		return "INT96"
	case parquet.Float:
		return "FLOAT32"
	case parquet.Double:
		return "FLOAT64"
	case parquet.ByteArray:
		return "BYTE_ARRAY"
	case parquet.FixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		return "UNKNOWN"
	}
}
