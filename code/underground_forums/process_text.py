import re
from difflib import SequenceMatcher
from collections import Counter
from statistics import mean
import pandas as pd

def preprocess_text(text):
    """Remove punctuation and numbers, convert to lowercase."""
    return re.sub(r'[^\w\s]', '', text.lower())

def tokenize(text):
    """Split text into words."""
    return preprocess_text(text).split()

def calculate_word_similarity(tokens1, tokens2):
    """Calculate similarity based on shared words, ignoring order."""
    count1 = Counter(tokens1)
    count2 = Counter(tokens2)
    shared_words = sum((count1 & count2).values())
    total_words = sum((count1 | count2).values())
    return shared_words / total_words if total_words > 0 else 0

def calculate_sequence_similarity(text1, text2):
    """Calculate similarity based on sequence (order of words)."""
    return SequenceMatcher(None, text1, text2).ratio()
    
def analyze_texts(data, text_column="body", author_column="username", other_column='Platform', similarity_threshold=0.8):
    # Extract texts and authors based on column names
    items = [
        (item[text_column], item[author_column], item[other_column]) 
        for item in data 
        if item.get(text_column) is not None
    ]

    # Check if the list of items is empty
    if not items:
        return {}

    texts, authors, others = zip(*items)
    preprocessed_texts = [preprocess_text(text) for text in texts]
    tokenized_texts = [tokenize(text) for text in preprocessed_texts]

    # Calculate average number of words in the text
    avg_words = mean(len(tokens) for tokens in tokenized_texts)

    # Calculate pairwise similarities
    word_similarities = []
    sequence_similarities = []
    similar_pairs = []  # Store pairs of texts with authors for high similarity

    for i in range(len(preprocessed_texts)):
        for j in range(i + 1, len(preprocessed_texts)):
            word_similarity = calculate_word_similarity(tokenized_texts[i], tokenized_texts[j])
            sequence_similarity = calculate_sequence_similarity(preprocessed_texts[i], preprocessed_texts[j])
            word_similarities.append(word_similarity)
            sequence_similarities.append(sequence_similarity)

            # Check if this pair is highly similar based on sequence
            if sequence_similarity >= similarity_threshold:
                similar_pairs.append({
                    "text_1": texts[i],
                    "author_1": authors[i],
                    "other_1": others[i],
                    "text_2": texts[j],
                    "author_2": authors[j],
                    "other_2": others[j],
                    "sequence_similarity": sequence_similarity
                })

    # Calculate averages
    avg_word_similarity = mean(word_similarities) if word_similarities else 0
    avg_sequence_similarity = mean(sequence_similarities) if sequence_similarities else 0

    return {
        "average_words_per_text": avg_words,
        "average_word_similarity": avg_word_similarity,
        "average_sequence_similarity": avg_sequence_similarity,
        "similar_pairs": similar_pairs  # List of highly similar text pairs with authors
    }
    
def load_data_from_csv(file_path):
    """Load data from a CSV file. Assumes columns: 'body', 'username', 'platform (title)', 'Platform'."""
    df = pd.read_csv(file_path)
    # Ensure required columns are present
    if not {'body', 'username', 'platform (title)', 'Platform'}.issubset(df.columns):
        raise ValueError("CSV file must contain 'text', 'author', and 'platform' columns.")
    return df
    
def count_author_and_other_combinations(similar_pairs):
    """
    Counts occurrences based on author and 'other' field combinations.
    - same author, same 'other'
    - different author, same 'other'
    - different author, different 'other'

    Parameters:
        similar_pairs (list): List of dictionaries containing similar text pairs.

    Returns:
        dict: Counts of the three cases.
    """
    same_author_same_other = 0
    different_author_same_other = 0
    different_author_different_other = 0

    for pair in similar_pairs:
        same_author = pair["author_1"] == pair["author_2"]
        same_other = pair["other_1"] == pair["other_2"]

        if same_author and same_other:
            same_author_same_other += 1
        elif not same_author and same_other:
            different_author_same_other += 1
        elif not same_author and not same_other:
            different_author_different_other += 1

    return {
        "same_author_same_other": same_author_same_other,
        "different_author_same_other": different_author_same_other,
        "different_author_different_other": different_author_different_other
    }
    


# Example usage
if __name__ == "__main__":
    # Specify the path to your CSV file
    csv_file_path = "data.csv"  # Replace with your file path
    df = load_data_from_csv(csv_file_path)
    
    # Drop rows with a specific forum value
    df = df[df['Platform'] != 'hackforums.net']
    df = df[df['Platform'] != 'swapd']
    df = df[df['Platform'] != 'cracked.io']
    
    df = df[df['body'].notna()]
    
    results_by_forum = {}
    for forum, group in df.groupby('Platform'):
        print(forum)
        group_data = group.to_dict('records')
        results_by_forum[forum] = analyze_texts(
            group_data,
            text_column="body",
            author_column="username",
            other_column="platform (title)"
        )
        if "similar_pairs" in results_by_forum[forum] and results_by_forum[forum]["similar_pairs"]:
            counts = count_author_and_other_combinations(results_by_forum[forum]["similar_pairs"])
            # Merge results of analyze_texts with counts
            results_by_forum[forum]["post_processing_counts"] = counts
    
    # Display the results
    for forum, results in results_by_forum.items():
        print(f"Forum: {forum}")
        print(f"Average Words Per Text: {results.get('average_words_per_text', 0):.2f}")
        print(f"Average Word Similarity: {results.get('average_word_similarity', 0):.2f}")
        print(f"Average Sequence Similarity: {results.get('average_sequence_similarity', 0):.2f}")
        
        # Print similar pairs, limiting text to 20 characters
        if "similar_pairs" in results and results["similar_pairs"]:
            print("Similar Pairs:")
            for pair in results["similar_pairs"]:
                text_1_snippet = pair['text_1'][:50] + ('...' if len(pair['text_1']) > 50 else '')
                text_2_snippet = pair['text_2'][:50] + ('...' if len(pair['text_2']) > 50 else '')
                print(f"  Text 1: {text_1_snippet} (Author: {pair['author_1']}, Social Netw: {pair['other_1']})")
                print(f"  Text 2: {text_2_snippet} (Author: {pair['author_2']}, Social Netw: {pair['other_2']})")
                print(f"  Similarity: {pair['sequence_similarity']:.2f}")
        if "post_processing_counts" in results:
            print("Post-Processing Counts:")
            for key, count in results["post_processing_counts"].items():
                print(f"  {key.replace('_', ' ').title()}: {count}")
        else:
            print("No similar pairs found.")
        print("-" * 50)
        
        

    results_by_sn = {}
    for sn, group in df.groupby('platform (title)'):
        print(sn)
        group_data = group.to_dict('records')
        results_by_sn[sn] = analyze_texts(
            group_data,
            text_column="body",
            author_column="username",
            other_column="Platform"
        )
        if "similar_pairs" in results_by_sn[sn] and results_by_sn[sn]["similar_pairs"]:
            counts = count_author_and_other_combinations(results_by_sn[sn]["similar_pairs"])
            # Merge results of analyze_texts with counts
            results_by_sn[sn]["post_processing_counts"] = counts
    
    # Display the results
    for sn, results in results_by_sn.items():
        print(f"Social Network: {sn}")
        print(f"Average Words Per Text: {results.get('average_words_per_text', 0):.2f}")
        print(f"Average Word Similarity: {results.get('average_word_similarity', 0):.2f}")
        print(f"Average Sequence Similarity: {results.get('average_sequence_similarity', 0):.2f}")
        
        # Print similar pairs, limiting text to 20 characters
        if "similar_pairs" in results and results["similar_pairs"]:
            print("Similar Pairs:")
            for pair in results["similar_pairs"]:
                text_1_snippet = pair['text_1'][:50] + ('...' if len(pair['text_1']) > 50 else '')
                text_2_snippet = pair['text_2'][:50] + ('...' if len(pair['text_2']) > 50 else '')
                print(f"  Text 1: {text_1_snippet} (Author: {pair['author_1']}, Platform: {pair['other_1']})")
                print(f"  Text 2: {text_2_snippet} (Author: {pair['author_2']}, Platform: {pair['other_2']})")
                print(f"  Similarity: {pair['sequence_similarity']:.2f}")
                # Display post-processing counts
        if "post_processing_counts" in results:
            print("Post-Processing Counts:")
            for key, count in results["post_processing_counts"].items():
                print(f"  {key.replace('_', ' ').title()}: {count}")
        else:
            print("No similar pairs found.")
        print("-" * 50)
        
