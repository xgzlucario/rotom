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
			Model: openai.GPT3Dot5Turbo0301,
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
