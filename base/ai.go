package base

import (
	"context"

	openai "github.com/sashabaranov/go-openai"
)

var client *openai.Client

func init() {
	client = openai.NewClient("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")
}

func Chat(content string) (string, error) {
	// send chat
	resp, err := client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model: openai.GPT3Dot5Turbo,
			Messages: []openai.ChatCompletionMessage{
				{
					Role:    openai.ChatMessageRoleUser,
					Content: content,
				},
			},
		},
	)
	if err != nil {
		return "", err
	}

	return resp.Choices[0].Message.Content, nil
}
