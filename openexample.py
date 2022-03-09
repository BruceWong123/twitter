import os
import openai


openai.api_key = "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"
completion = openai.Completion()

# start_chat_log = '''Human: Hello, who are you?
# AI: I am doing great. How can I help you today?
# Human: I wish to get something to eat, do you have any?
# AI: I don't
# '''

start_chat_log = ''


def ask(question, chat_log=None):
    if chat_log is None:
        chat_log = start_chat_log
    prompt = f'{chat_log}Human: {question}\nAI:'
    response = completion.create(
        prompt=prompt, engine="ada", stop=['\nHuman'], temperature=0.9,
        top_p=1, frequency_penalty=0, presence_penalty=0.6, best_of=1,
        max_tokens=150)
    answer = response.choices[0].text.strip()
    return answer


print(ask('Have you heard of NFT?'))
