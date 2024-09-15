import streamlit as st
import pandas as pd
import plotly.express as px
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
from db_connection import fetch_tweets_data
import time

TIME_ORDER_FIELD = "tweet_timestamp"

st.title("Sentiment Analysis of Tweets about US Airlines")
st.sidebar.title("Sentiment Analysis Controls")


# Cache the data, but without TTL due to persistence
@st.cache_data(persist=True)
def load_data():
    data = fetch_tweets_data()
    return data

# Function to calculate the change in sentiment percentages
def calculate_sentiment_change(current_data, previous_data):
    current_sentiment_count = current_data['airline_sentiment'].value_counts(normalize=True) * 100
    previous_sentiment_count = previous_data['airline_sentiment'].value_counts(normalize=True) * 100

    # Get percentage change
    change = current_sentiment_count - previous_sentiment_count
    return current_sentiment_count, change

# Initialize session state to store previous data if it doesn't exist
if 'previous_data' not in st.session_state:
    st.session_state.previous_data = pd.DataFrame()

# Add a "Reload" button to refresh the data manually
if st.sidebar.button("Reload Data"):
    # Clear cached data to force a refresh
    st.cache_data.clear()

# Load the data
current_data = load_data()

# Track total number of tweets and sentiment changes
total_tweets = len(current_data)
if not st.session_state.previous_data.empty:
    # Calculate the difference in total number of tweets
    previous_tweets = len(st.session_state.previous_data)
    tweet_increase = total_tweets - previous_tweets

    # Calculate sentiment changes
    current_sentiment, sentiment_change = calculate_sentiment_change(current_data, st.session_state.previous_data)

    # Display metrics in a horizontal row using st.columns
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tweets", total_tweets, f"+{tweet_increase}")
    col2.metric("Positive Tweets", f"{current_sentiment.get('positive', 0):.2f}%", f"{sentiment_change.get('positive', 0):+.2f}%")
    col3.metric("Neutral Tweets", f"{current_sentiment.get('neutral', 0):.2f}%", f"{sentiment_change.get('neutral', 0):+.2f}%")
    col4.metric("Negative Tweets", f"{current_sentiment.get('negative', 0):.2f}%", f"{sentiment_change.get('negative', 0):+.2f}%")
else:
    # Display initial metrics when no previous data is available
    current_sentiment = current_data['airline_sentiment'].value_counts(normalize=True) * 100

    # Display initial metrics with no deltas
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tweets", total_tweets)
    col2.metric("Positive Tweets", f"{current_sentiment.get('positive', 0):.2f}%")
    col3.metric("Neutral Tweets", f"{current_sentiment.get('neutral', 0):.2f}%")
    col4.metric("Negative Tweets", f"{current_sentiment.get('negative', 0):.2f}%")

# Update session state to store the current data for the next refresh
st.session_state.previous_data = current_data

# Sidebar settings
st.sidebar.markdown("This application is a Streamlit dashboard to analyse the sentiment of Tweets")

st.sidebar.subheader("Show random tweet")
random_tweet = st.sidebar.radio("Sentiment", ("positive", "negative", "neutral"))
tweet_text = current_data.query("sentiment_label == @random_tweet")[["text"]].sample(1).iat[0, 0]
st.sidebar.markdown(tweet_text)

# Create tabs for the main visualizations
tabs = st.tabs(["Sentiment Overview", "Airline Breakdown", "Word Cloud", "Sampled Tweets"])

with tabs[0]:
    # Sentiment Overview: Number of tweets by sentiment
    st.markdown("### Number of tweets by sentiment")
    select = st.selectbox("Visualisation type", ["Histogram", "Pie chart"], key="1")
    sentiment_count = current_data['sentiment_label'].value_counts()
    sentiment_count = pd.DataFrame({"Sentiment": sentiment_count.index, "Tweets": sentiment_count.values})

    if select == "Histogram":
        fig = px.bar(sentiment_count, x='Sentiment', y='Tweets', color="Tweets")
    else:
        fig = px.pie(sentiment_count, values="Tweets", names="Sentiment")
    st.plotly_chart(fig)

with tabs[1]:
    # Airline Breakdown: Breakdown airline tweets by sentiment
    st.markdown("### Breakdown airline tweets by sentiment")
    choice = st.multiselect('Pick airlines', tuple(current_data.airline.unique()), key="0")

    if len(choice) > 0:
        choice_data = current_data[current_data.airline.isin(choice)]
        fig_choice = px.histogram(choice_data, x='airline', y='sentiment_label', histfunc='count', color='airline_sentiment',
                                  facet_col='sentiment_label', labels={'sentiment_label': 'tweets'}, height=600, width=800)
        st.plotly_chart(fig_choice)

with tabs[2]:
    # Word Cloud: Display word clouds for different sentiments
    st.markdown("### Word Cloud for Selected Sentiment")
    word_sentiment = st.radio("Display word cloud for which sentiment?", ("positive", "negative", "neutral"))

    df = current_data[current_data['sentiment_label'] == word_sentiment]
    words = ' '.join(df['text'])
    processed_words = ' '.join([word for word in words.split() if "http" not in word and not word.startswith("@") and word != "RT"])
    wordcloud = WordCloud(stopwords=STOPWORDS, background_color='white', height=640, width=800).generate(processed_words)
    plt.imshow(wordcloud)
    plt.xticks([])
    plt.yticks([])
    fig_wc = plt.gcf()
    st.pyplot(fig_wc)

with tabs[3]:
    # Sampled Tweets: Display a random sample of up to 10 tweets as a dataframe or JSON
    st.markdown("### Sampled Tweets")
    
    # Radio button for choosing between dataframe or JSON
    view_format = st.radio("View format:", ("DataFrame", "JSON"), index=0)
    
    # Sample up to 10 tweets from the data
    sampled_tweets = current_data.sample(10)

    # Display tweets in the selected format
    if view_format == "DataFrame":
        st.dataframe(sampled_tweets)
    else:
        st.json(sampled_tweets.to_dict(orient='records'))