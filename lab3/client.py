# client.py
from xmlrpc.client import ServerProxy
import time

class Client:
    def __init__(self):
        self.coordinator = ServerProxy("http://localhost:8001")
        self.node2 = ServerProxy("http://localhost:8002")
        self.node3 = ServerProxy("http://localhost:8003")
    
    def reset_for_scenario(self, scenario):
        """Reset balances for different test scenarios"""
        print(f"\nResetting for scenario {scenario}")
        self.node2.reset_balance(scenario)
        self.node3.reset_balance(scenario)
        print("Initial balances after reset:")
        self.print_balances()
        print("-" * 50)

    def print_balances(self):
        try:
            print(f"Account A balance: {self.node2.get_balance()}")
            print(f"Account B balance: {self.node3.get_balance()}")
        except Exception as e:
            print(f"Error getting balances: {e}")
        
    def execute_transaction(self, transaction, crash_scenario=None):
        print(f"\nExecuting transaction: {transaction}")
        if crash_scenario:
            print(f"With crash scenario: {crash_scenario}")
        print("Initial balances:")
        self.print_balances()
        
        # Set up crash scenario if specified
        if crash_scenario == 'c1':
            self.node2.set_crash_scenario('c1', 'before')
        elif crash_scenario == 'c2':
            self.node2.set_crash_scenario('c2', 'after')
        
        # Phase 1: Prepare
        try:
            prepare_a = self.node2.prepare(transaction)
            if not prepare_a:
                if transaction['type'] == 'transfer':
                    print(f"Transaction failed: Insufficient funds in Account A for transfer of {transaction['amount']}")
                else:
                    print("Transaction failed: Account A cannot prepare for the transaction")
                self.node2.abort()
                self.node3.abort()
                return False

            prepare_b = self.node3.prepare(transaction)
            if not prepare_b:
                print("Transaction failed: Account B cannot prepare for the transaction")
                self.node2.abort()
                self.node3.abort()
                return False
            
            if prepare_a and prepare_b:
                # Simulate coordinator crash before commit if specified
                if crash_scenario == 'c3':
                    print("Simulating coordinator crash for 30 seconds...")
                    time.sleep(30)
                
                # Phase 2: Commit
                commit_a = self.node2.commit(transaction)
                commit_b = self.node3.commit(transaction)
                
                if commit_a and commit_b:
                    print("Transaction completed successfully")
                else:
                    self.node2.abort()
                    self.node3.abort()
                    print("Transaction aborted during commit phase")
            else:
                self.node2.abort()
                self.node3.abort()
                print("Transaction aborted during prepare phase")
                
        except Exception as e:
            print(f"Error during transaction: {e}")
            try:
                self.node2.abort()
                self.node3.abort()
            except:
                print("Could not abort transaction on all nodes")
            print("Transaction aborted due to error")
            
        print("Final balances:")
        self.print_balances()
        print("-" * 50)
        return True

def print_menu():
    print("\n=== 2PC Transaction Simulator ===")
    print("1. Scenario A: Normal operation (A=200, B=300)")
    print("2. Scenario B: Insufficient funds (A=90, B=50)")
    print("3. Scenario C: Node crash simulation (A=200, B=300)")
    print("4. Custom transaction")
    print("5. Print current balances")
    print("6. Exit")

def handle_scenario_c():
    print("\nScenario C options:")
    print("1. Node-2 crashes before responding")
    print("2. Node-2 crashes after responding")
    print("3. Coordinator crashes after sending request")
    return input("Choose crash scenario (1-3): ")

def main():
    client = Client()
    
    while True:
        print_menu()
        choice = input("\nEnter your choice (1-6): ")
        
        if choice == '1':
            client.reset_for_scenario('a')
            print("\nChoose transaction type:")
            print("1. Transfer 100 from A to B")
            print("2. Add 20% bonus to both accounts")
            sub_choice = input("Enter choice (1-2): ")
            
            if sub_choice == '1':
                client.execute_transaction({
                    'type': 'transfer',
                    'amount': 100
                })
            elif sub_choice == '2':
                client.execute_transaction({
                    'type': 'bonus'
                })
                
        elif choice == '2':
            client.reset_for_scenario('b')
            print("\nChoose transaction type:")
            print("1. Transfer 100 from A to B (should fail)")
            print("2. Add 20% bonus to both accounts")
            sub_choice = input("Enter choice (1-2): ")
            
            if sub_choice == '1':
                client.execute_transaction({
                    'type': 'transfer',
                    'amount': 100
                })
            elif sub_choice == '2':
                client.execute_transaction({
                    'type': 'bonus'
                })
                
        elif choice == '3':
            client.reset_for_scenario('c')
            crash_choice = handle_scenario_c()
            print("\nChoose transaction type:")
            print("1. Transfer 100 from A to B")
            print("2. Add 20% bonus to both accounts")
            sub_choice = input("Enter choice (1-2): ")
            
            transaction = {
                'type': 'transfer' if sub_choice == '1' else 'bonus'
            }
            if sub_choice == '1':
                transaction['amount'] = 100
                
            if crash_choice == '1':
                client.execute_transaction(transaction, 'c1')
            elif crash_choice == '2':
                client.execute_transaction(transaction, 'c2')
            elif crash_choice == '3':
                client.execute_transaction(transaction, 'c3')
            
        elif choice == '4':
            print("\nCustom Transaction:")
            tx_type = input("Enter transaction type (transfer/bonus): ").lower()
            if tx_type == 'transfer':
                amount = float(input("Enter transfer amount: "))
                client.execute_transaction({
                    'type': 'transfer',
                    'amount': amount
                })
            elif tx_type == 'bonus':
                client.execute_transaction({
                    'type': 'bonus'
                })
        
        elif choice == '5':
            client.print_balances()
            
        elif choice == '6':
            print("Exiting simulator...")
            break
            
        else:
            print("Invalid choice! Please try again.")

if __name__ == "__main__":
    main()