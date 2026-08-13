package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Alhamdulillah-R/memory-recall-coin/internal/service"
)

const (
	callToolMethod           = "tools/call"
	argumentValidationPrefix = `validating "arguments":`
)

var validationFieldAliases = map[string]string{
	"ingest_id":   "ingestion_id",
	"query_mode":  "retrieval_mode",
	"search_mode": "retrieval_mode",
	"source_path": "path",
}

// toolSchemaCatalog keeps the final input schema advertised for each MCP tool.
type toolSchemaCatalog map[string]*jsonschema.Schema

type validationFieldError struct {
	Field      string `json:"field"`
	Reason     string `json:"reason"`
	Suggestion string `json:"suggestion,omitempty"`
	Allowed    []any  `json:"allowed,omitempty"`
}

/**
 * validationErrorMiddleware replaces SDK argument validation text with a self-correctable tool error.
 * @param catalog final input schemas keyed by tool name
 * @return MCP receiving middleware
 */
func validationErrorMiddleware(catalog toolSchemaCatalog) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, request)
			if err != nil || method != callToolMethod {
				return result, err
			}

			callResult, ok := result.(*mcp.CallToolResult)
			if !ok || !callResult.IsError || callResult.StructuredContent != nil {
				return result, nil
			}
			rawValidationError, ok := argumentValidationError(callResult)
			if !ok {
				return result, nil
			}

			params, ok := request.GetParams().(*mcp.CallToolParamsRaw)
			if !ok || params == nil {
				return result, nil
			}
			schema := catalog[params.Name]
			if schema == nil {
				return result, nil
			}

			arguments, argumentErr := decodeToolArguments(params.Arguments)
			errors := diagnoseValidationErrors(schema, arguments, argumentErr)
			details := validationErrorDetails(
				params.Name,
				schema,
				errors,
				rawValidationError,
			)
			payload := toolError{
				Code:    service.CodeInvalidArgument,
				Message: "arguments failed schema validation",
				Details: details,
			}

			enhancedResult := *callResult
			enhancedResult.Content = []mcp.Content{
				&mcp.TextContent{Text: validationErrorSummary(params.Name, schema, errors)},
			}
			enhancedResult.StructuredContent = payload

			return &enhancedResult, nil
		}
	}
}

func argumentValidationError(result *mcp.CallToolResult) (string, bool) {
	if validationErr := result.GetError(); validationErr != nil {
		message := validationErr.Error()
		if strings.HasPrefix(message, argumentValidationPrefix) {
			return message, true
		}
	}

	for _, content := range result.Content {
		textContent, ok := content.(*mcp.TextContent)
		if !ok || !strings.HasPrefix(textContent.Text, argumentValidationPrefix) {
			continue
		}

		return textContent.Text, true
	}

	return "", false
}

func decodeToolArguments(raw json.RawMessage) (map[string]json.RawMessage, error) {
	arguments := make(map[string]json.RawMessage)
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return arguments, nil
	}
	if err := json.Unmarshal(raw, &arguments); err != nil {
		return nil, err
	}

	return arguments, nil
}

func diagnoseValidationErrors(
	schema *jsonschema.Schema,
	arguments map[string]json.RawMessage,
	argumentErr error,
) []validationFieldError {
	if argumentErr != nil {
		return []validationFieldError{{
			Field:      "$",
			Reason:     "invalid_json",
			Suggestion: "send arguments as one JSON object",
		}}
	}

	errors := make([]validationFieldError, 0)
	errors = append(errors, unknownFieldErrors(schema, arguments)...)
	errors = append(errors, missingRequiredErrors(schema, arguments)...)
	errors = append(errors, invalidEnumErrors(schema, arguments)...)
	errors = append(errors, mutuallyExclusiveNamespaceErrors(schema, arguments)...)

	return errors
}

func unknownFieldErrors(
	schema *jsonschema.Schema,
	arguments map[string]json.RawMessage,
) []validationFieldError {
	fields := sortedArgumentFields(arguments)
	errors := make([]validationFieldError, 0)
	for _, field := range fields {
		if _, exists := schema.Properties[field]; exists {
			continue
		}

		suggestion := suggestedProperty(field, schema.Properties)
		reason := "unsupported_by_tool"
		if suggestion != "" {
			reason = "unknown_field"
		}
		errors = append(errors, validationFieldError{
			Field:      field,
			Reason:     reason,
			Suggestion: suggestion,
		})
	}

	return errors
}

func missingRequiredErrors(
	schema *jsonschema.Schema,
	arguments map[string]json.RawMessage,
) []validationFieldError {
	required := append([]string(nil), schema.Required...)
	sort.Strings(required)

	errors := make([]validationFieldError, 0)
	for _, field := range required {
		if _, exists := arguments[field]; exists {
			continue
		}
		errors = append(errors, validationFieldError{
			Field:  field,
			Reason: "required",
		})
	}

	return errors
}

func invalidEnumErrors(
	schema *jsonschema.Schema,
	arguments map[string]json.RawMessage,
) []validationFieldError {
	fields := sortedArgumentFields(arguments)
	errors := make([]validationFieldError, 0)
	for _, field := range fields {
		property := schema.Properties[field]
		if property == nil || len(property.Enum) == 0 {
			continue
		}

		var value any
		if err := json.Unmarshal(arguments[field], &value); err != nil {
			continue
		}
		if enumContains(property.Enum, value) {
			continue
		}

		errors = append(errors, validationFieldError{
			Field:   field,
			Reason:  "invalid_enum",
			Allowed: append([]any(nil), property.Enum...),
		})
	}

	return errors
}

func mutuallyExclusiveNamespaceErrors(
	schema *jsonschema.Schema,
	arguments map[string]json.RawMessage,
) []validationFieldError {
	pairs := [][2]string{
		{"namespace", "namespace_sequence"},
		{"parent", "parent_sequence"},
	}
	errors := make([]validationFieldError, 0)
	for _, pair := range pairs {
		if _, exists := schema.Properties[pair[0]]; !exists {
			continue
		}
		if _, exists := schema.Properties[pair[1]]; !exists {
			continue
		}
		if _, exists := arguments[pair[0]]; !exists {
			continue
		}
		if _, exists := arguments[pair[1]]; !exists {
			continue
		}

		errors = append(errors, validationFieldError{
			Field:      pair[0] + "," + pair[1],
			Reason:     "mutually_exclusive",
			Suggestion: fmt.Sprintf("provide exactly one of %s or %s", pair[0], pair[1]),
		})
	}

	return errors
}

func validationErrorDetails(
	toolName string,
	schema *jsonschema.Schema,
	errors []validationFieldError,
	rawValidationError string,
) map[string]any {
	details := map[string]any{
		"errors":               errors,
		"schema_version":       toolName + "@1",
		"raw_validation_error": rawValidationError,
	}
	if requiredOneOf := requiredChoice(schema.OneOf); len(requiredOneOf) > 0 {
		details["required_one_of"] = requiredOneOf
	}
	if requiredAnyOf := requiredChoice(schema.AnyOf); len(requiredAnyOf) > 0 {
		details["required_any_of"] = requiredAnyOf
	}
	if len(schema.Examples) > 0 {
		details["example"] = schema.Examples[0]
	}

	return details
}

func requiredChoice(alternatives []*jsonschema.Schema) []string {
	if len(alternatives) < 2 {
		return nil
	}

	fields := make([]string, 0, len(alternatives))
	seen := make(map[string]struct{}, len(alternatives))
	for _, alternative := range alternatives {
		if alternative == nil || len(alternative.Required) != 1 {
			return nil
		}

		field := alternative.Required[0]
		if _, exists := seen[field]; exists {
			continue
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	if len(fields) < 2 {
		return nil
	}

	return fields
}

func suggestedProperty(field string, properties map[string]*jsonschema.Schema) string {
	fieldLower := strings.ToLower(field)
	if alias := validationFieldAliases[fieldLower]; alias != "" {
		if _, exists := properties[alias]; exists {
			return alias
		}
	}

	propertyNames := make([]string, 0, len(properties))
	for propertyName := range properties {
		propertyNames = append(propertyNames, propertyName)
	}
	sort.Strings(propertyNames)

	bestProperty := ""
	bestDistance := -1
	for _, propertyName := range propertyNames {
		distance := levenshteinDistance(fieldLower, strings.ToLower(propertyName))
		if bestDistance >= 0 && distance >= bestDistance {
			continue
		}
		bestProperty = propertyName
		bestDistance = distance
	}
	if bestDistance < 0 || bestDistance > suggestionDistanceLimit(fieldLower) {
		return ""
	}

	return bestProperty
}

func suggestionDistanceLimit(field string) int {
	length := len([]rune(field))
	if length <= 4 {
		return 1
	}
	if length <= 12 {
		return 2
	}

	return 3
}

func levenshteinDistance(first, second string) int {
	firstRunes := []rune(first)
	secondRunes := []rune(second)
	previous := make([]int, len(secondRunes)+1)
	current := make([]int, len(secondRunes)+1)
	for index := range previous {
		previous[index] = index
	}

	for firstIndex, firstRune := range firstRunes {
		current[0] = firstIndex + 1
		for secondIndex, secondRune := range secondRunes {
			cost := 1
			if firstRune == secondRune {
				cost = 0
			}

			deletion := previous[secondIndex+1] + 1
			insertion := current[secondIndex] + 1
			substitution := previous[secondIndex] + cost
			current[secondIndex+1] = minimumInt(deletion, insertion, substitution)
		}
		previous, current = current, previous
	}

	return previous[len(secondRunes)]
}

func minimumInt(values ...int) int {
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}

func enumContains(allowed []any, value any) bool {
	for _, candidate := range allowed {
		if reflect.DeepEqual(candidate, value) {
			return true
		}
	}

	return false
}

func sortedArgumentFields(arguments map[string]json.RawMessage) []string {
	fields := make([]string, 0, len(arguments))
	for field := range arguments {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	return fields
}

func validationErrorSummary(
	toolName string,
	schema *jsonschema.Schema,
	errors []validationFieldError,
) string {
	if len(errors) == 0 {
		if requiredAnyOf := requiredChoice(schema.AnyOf); len(requiredAnyOf) > 0 {
			return fmt.Sprintf(
				"INVALID_ARGUMENT: %s requires at least one of %s",
				toolName,
				strings.Join(requiredAnyOf, ", "),
			)
		}
		if requiredOneOf := requiredChoice(schema.OneOf); len(requiredOneOf) > 0 {
			return fmt.Sprintf(
				"INVALID_ARGUMENT: %s requires exactly one of %s",
				toolName,
				strings.Join(requiredOneOf, ", "),
			)
		}

		return fmt.Sprintf(
			"INVALID_ARGUMENT: %s arguments are invalid; inspect structured details and retry",
			toolName,
		)
	}

	firstError := errors[0]
	if firstError.Suggestion != "" {
		return fmt.Sprintf(
			"INVALID_ARGUMENT: %s field %q is invalid; %s",
			toolName,
			firstError.Field,
			firstError.Suggestion,
		)
	}
	if len(firstError.Allowed) > 0 {
		return fmt.Sprintf(
			"INVALID_ARGUMENT: %s field %q is invalid; use an allowed value",
			toolName,
			firstError.Field,
		)
	}

	return fmt.Sprintf(
		"INVALID_ARGUMENT: %s field %q is %s; inspect structured details and retry",
		toolName,
		firstError.Field,
		firstError.Reason,
	)
}
