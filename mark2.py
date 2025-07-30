import os
import gradio as gr
from langchain_community.graphs import Neo4jGraph
from langchain_community.chat_models import ChatOllama
from langchain.chains import GraphCypherQAChain
from langchain_huggingface import HuggingFaceEndpoint
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.agents import AgentExecutor, create_react_agent, Tool
from opik.integrations.langchain import OpikTracer


URI = os.environ.get('URI')
USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')

OPIK_API_KEY = os.environ.get('OPIK_API_KEY')
OPIK_WORKSPACE = os.environ.get('OPIK_WORKSPACE')


CYPHER_GENERATION_TEMPLATE = """
Task: You are an expert Neo4j Cypher translator. Your sole purpose is to generate a single, syntactically correct Cypher statement to query a graph database.
Instructions:
- Use only the relationship types and properties provided in the schema. Do not use any others.
- To filter on properties, you MUST use a `WHERE` clause.
- For case-insensitive filtering, you MUST convert the property to lowercase using `toLower()`.
- **Crucially, you MUST use the `CONTAINS` keyword for all name filtering.** For example: `MATCH (p:Player) WHERE toLower(p.name) CONTAINS 'spirit'`. Do NOT use the `=` operator for names.
- Only return the specific property requested by the user.
- Do not include any explanations, apologies, or any text other than the Cypher query itself.

Schema:
{schema}

The user's question is:
{question}
"""
CYPHER_PROMPT = PromptTemplate(
    input_variables=["schema", "question"], template=CYPHER_GENERATION_TEMPLATE
)

AGENT_PROMPT_TEMPLATE = """
You are an agent designed to answer questions about Dota 2 eSports.
You have access to a single tool for this purpose.
Given the user's question, you MUST use the tool to find the answer.

{tools}

Use the following format:

Question: the input question you must answer
Thought: I must use the Graph Database tool to answer this question about Dota 2.
Action: the action to take, should be one of {tool_names}
Action Input: the user's original question as a string
Observation: the result of the action
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Previous conversation history:
{chat_history}

New question: {input}
{agent_scratchpad}
"""
AGENT_PROMPT = PromptTemplate.from_template(AGENT_PROMPT_TEMPLATE)


opik_tracer = OpikTracer()

graph = Neo4jGraph(url=URI, username=USERNAME, password=PASSWORD)
graph.refresh_schema()

llm = ChatOllama(model="mistral:7b", temperature=0)

chain = GraphCypherQAChain.from_llm(
    graph=graph,
    llm=llm,
    cypher_prompt=CYPHER_PROMPT,
    verbose=True,
    allow_dangerous_requests=True
)

tools = [
    Tool(
        name="Graph Database",
        func=chain.invoke,
        description="Useful for answering questions about Dota 2 players, teams, and tournaments"
    )
]

memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
agent = create_react_agent(llm, tools, AGENT_PROMPT)
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    memory=memory,
    verbose=True,
    handle_parsing_errors=True
)
print("Agent initialized.")

def chat_function(message, history):
    try:
        result = agent_executor.invoke({'input': message}, config={'callbacks': [opik_tracer]})
        return result['output']
    except Exception as e:
        return f'Error occured: {e}'
    
iface = gr.ChatInterface(
    fn=chat_function,
    title='Dota2AI',
    description='For now ask only about Dota2 tier 1 teams, matches, leagues',
    examples=[
        ['Who plays for Team Spirit?'],
        ['Which team participated in TI12?']
    ]
)    

if __name__=='__main__':
    iface.launch()