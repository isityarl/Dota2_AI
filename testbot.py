import os
from langchain_community.graphs import Neo4jGraph
from langchain.chains import GraphCypherQAChain
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts.prompt import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.agents import AgentExecutor, create_react_agent, Tool

URI = 'neo4j+s://9fa0005b.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '2OGzHvL34RYo6zDxtqKxsWl6ktfkjVpvjV5_Hod-Mq8'


CYPHER_GENERATION_TEMPLATE = """
Task:Generate Cypher statement to query a graph database.
Instructions:
Use only the provided relationship types and properties in the schema.
Do not use any other relationship types or properties that are not provided.
Schema:
{schema}

Note: Do not include any explanations or apologies in your response.
Do not respond to questions that might ask for anything else than a Cypher statement.
Do not include any text except the generated Cypher statement.

A crucial rule is to use the `CONTAINS` keyword for fuzzy matching of names in `Player`, `Team`, and `Tournament` nodes. For example, if the user asks about "Spirit", you should search for names containing "Spirit".

The question is:
{question}
"""
CYPHER_PROMPT = PromptTemplate(
    input_variables=["schema", "question"], template=CYPHER_GENERATION_TEMPLATE
)

# --- NEW: AGENT CONVERSATIONAL PROMPT ---
# This is the main prompt for the agent, telling it how to behave.
AGENT_PROMPT_TEMPLATE = """
You are a Dota 2 eSports expert with access to a graph database.
You are having a conversation with a human.
Given the following conversation history and a follow-up question,
decide whether to use the graph database tool to answer it.
You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Previous conversation history:
{chat_history}

New question: {input}
{agent_scratchpad}
"""

AGENT_PROMPT = PromptTemplate.from_template(AGENT_PROMPT_TEMPLATE)

def main():
    graph = Neo4jGraph(url=URI,
                       username=USERNAME,
                       password=PASSWORD)
    graph.refresh_schema()

    print(graph.schema)

    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0)

    chain = GraphCypherQAChain.from_llm(
        graph=graph,
        llm=llm,
        verbose=True,
        cypher_prompt=CYPHER_PROMPT,
        allow_dangerous_requests=True
    )
    
    tools = [
        Tool(
            name='Graph Database',
            func=chain.invoke,
            description='Useful for answering questions about Dota 2 players, teams, and tournaments' 
        )
    ]

    memory = ConversationBufferMemory(memory_key='chat_history')

    agent = create_react_agent(llm, tools, AGENT_PROMPT)
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        memory=memory,
        verbose=True,
        handle_parsing_errors=True
    )

    print('...Ready')
    print('Ask ur question')
    print('"exit" to exit')

    while True:
        question = input('> ')
        if question.lower() == 'exit': 
            break
        try:
            result = agent_executor.invoke({'input': question})
            print('\nAnswer:')
            print(result['output'])
            print('-' * 20)
        except Exception as e:
            print(e)

if __name__=='__main__':
    main()