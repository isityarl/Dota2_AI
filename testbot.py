import os
from langchain_community.graphs import Neo4jGraph
from langchain.chains import GraphCypherQAChain
from langchain_community.chat_models import ChatOllama
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.agents import AgentExecutor, create_react_agent, Tool

URI = 'neo4j+s://9fa0005b.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '2OGzHvL34RYo6zDxtqKxsWl6ktfkjVpvjV5_Hod-Mq8'


CYPHER_GENERATION_TEMPLATE = """
Task: You are an expert Neo4j Cypher translator. Generate a Cypher statement to query a graph database.
Instructions:
- Use only the relationship types and properties provided in the schema.
- To filter on properties, you MUST use a `WHERE` clause.
- **Correct syntax for filtering:** `MATCH (p:Player) WHERE p.name CONTAINS 'spirit'`
- Do not include any explanations or apologies in your response. Only provide the Cypher query.

Schema:
{schema}

The user's question is:
{question}
"""
CYPHER_PROMPT = PromptTemplate(
    input_variables=["schema", "question"], template=CYPHER_GENERATION_TEMPLATE
)

# --- AGENT CONVERSATIONAL PROMPT ---
AGENT_PROMPT_TEMPLATE = """
You are a helpful Dota 2 eSports expert. You are having a conversation with a human.
Given the conversation history and a follow-up question, use your tools to answer.
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

    llm = ChatOllama(model="llama3:8b", temperature=0)

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