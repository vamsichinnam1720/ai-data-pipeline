import os
from google import genai
from dotenv import load_dotenv

class SmartAssistant:
    """
    LLM-powered Smart Assistant using Google Gemini
    """
    def __init__(self):
        # We need an API key to initialize the client.
        # Ensure environment variables are loaded from .env
        load_dotenv()
        self.api_key = os.environ.get("GEMINI_API_KEY")
        if self.api_key:
            self.client = genai.Client(api_key=self.api_key)
        else:
            self.client = None

    def generate_response(self, user_query: str, context: str = "") -> str:
        """
        Generate a conversational response using Gemini.
        If no API key is provided, returns a placeholder message.
        """
        if not self.client:
            return "Hello! I am the AI Smart Assistant. However, it looks like the `GEMINI_API_KEY` environment variable is not set. Please add it to your `.env` file or system environment variables to enable my full capabilities!"

        try:
            # Construct a prompt with context
            prompt = (
                "You are a helpful data engineering AI assistant for a data pipeline application.\n\n"
                "CRITICAL FORMATTING RULES:\n"
                "- When presenting data with multiple rows or columns, ALWAYS format it as a clean HTML table (`<table>`, `<tr>`, `<th>`, `<td>`).\n"
                "- Do NOT use Markdown tables (e.g., using `|`).\n"
                "- Apply inline CSS to tables for a clear presence. For example: `<table style=\"border-collapse: collapse; width: 100%; margin-top: 10px;\">` and `<th style=\"border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;\">`.\n"
                "- Keep explanations concise and specific.\n\n"
            )
            if context:
                prompt += f"Here is some context about the user's current data:\n{context}\n\n"
            
            prompt += f"User query: {user_query}\n"
            
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
            )
            return response.text
        except Exception as e:
            return f"An error occurred while generating the response: {str(e)}"
